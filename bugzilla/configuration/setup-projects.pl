#!/usr/bin/perl
#
# setup-projects.pl - converge the internal Bugzilla installation onto the
# configuration declared in expected-state.json.
#
# Bugzilla exposes Product.create and Component.create over REST, but bug
# statuses, the status_workflow matrix, custom fields and field-value control
# have no WebService at all. So this runs inside a Bugzilla container and drives
# Bugzilla's own object APIs, with direct SQL used only where the 5.2 object
# layer cannot reach (status_workflow rows, and bug_status.is_open, which is
# absent from Bugzilla::Field::Choice::UPDATE_COLUMNS).
#
# It is idempotent: re-running it against a converged installation reports zero
# actions. Without --apply it makes no persistent change - it still opens a
# database connection and takes a server-side advisory lock, but writes nothing.
#
# It refuses to converge anything it does not fully understand: unexpected
# statuses, resolutions, field values or components abort the run rather than
# being silently tolerated, because a partially-declared installation is exactly
# what would make the verifier and this script disagree.
#
# Apache MUST be stopped while applying (see --web-host): with traffic flowing,
# enforcement would be observably off between the two ALTER TABLEs, and
# concurrent editworkflow.cgi/bug filing would race the workflow rewrite. The
# container's PID 1 is startup.sh's sleep loop, not Apache, so
# `docker exec ... apachectl stop` takes the site down without taking the
# container (and its localconfig) with it.
#
# See bugzilla/README.md for the full runbook.

BEGIN {
  my $root = $ENV{BUGZILLA_ROOT} || '/var/www/html';
  chdir($root) or die "FATAL: cannot chdir to Bugzilla root '$root': $!\n";
  unshift @INC, $root, "$root/lib", "$root/local/lib/perl5";
}

use 5.14.0;
use strict;
use warnings;

use Getopt::Long qw(GetOptions);
use IO::Socket::INET ();
use JSON::PP         ();

use Bugzilla;
use Bugzilla::Constants;
use Bugzilla::Config qw(:admin);
use Bugzilla::Component;
use Bugzilla::Field;
use Bugzilla::Field::Choice;
use Bugzilla::Milestone;
use Bugzilla::Product;
use Bugzilla::Status;
use Bugzilla::User;
use Bugzilla::Version;

BEGIN { Bugzilla->extensions }

use constant LOCK_NAME  => 'bugzilla-project-setup';

# Declared here rather than beside ensure_field() because preflight uses it too,
# and a file-scoped `my` is only visible to code compiled after it.
my %FIELD_TYPE = (single_select => FIELD_TYPE_SINGLE_SELECT,);

use constant EXTENSION  => 'Bugzilla::Extension::IssueTypeWorkflow';

my %OPT = (
  state                   => 'local/expected-state.json',
  'admin-login'           => undef,
  'default-assignee'      => undef,
  apply                   => 0,
  'remove-decommissioned' => 0,
  'web-host'              => 'localhost',
  'web-port'              => 80,
  'allow-live'            => 0,
  'skip-self-test'        => 0,
  help                    => 0,
);

GetOptions(\%OPT,
  'state=s', 'admin-login=s', 'default-assignee=s', 'apply!',
  'remove-decommissioned!', 'web-host=s', 'web-port=i', 'allow-live!',
  'skip-self-test!', 'help!')
  or die "FATAL: bad options. Try --help.\n";

if ($OPT{help}) {
  print <<'USAGE';
setup-projects.pl [options]

  --state FILE               expected-state.json. Relative paths resolve from the
                             Bugzilla root (BUGZILLA_ROOT, default /var/www/html),
                             which this script chdirs to. Default:
                             local/expected-state.json
  --admin-login LOGIN        existing, enabled Bugzilla admin to act as (required)
  --default-assignee LOGIN   default assignee for newly created components
                             (default: --admin-login)
  --apply                    perform the changes (default is a dry run)
  --remove-decommissioned    also retire the products listed under
                             "decommission" (fails if they still hold bugs)
  --web-host HOST            web tier to check is stopped before applying
                             (default: localhost - the script runs inside the
                             web container, where Apache is a separate daemon)
  --web-port PORT            port for that check (default: 80)
  --allow-live               apply even though the web tier is still serving.
                             UNSAFE - enforcement is observably off part-way
                             through, and config edits race the rewrite.
  --skip-self-test           do not run the post-apply fail-closed self-test
                             (it damages the workflow inside a transaction and
                             rolls it back, to prove enforcement really does
                             trip; there is no reason to skip it)
  --help                     this text
USAGE
  exit 0;
}

my @PLAN;                 # every action, planned or performed
my $APPLIED = 0;          # actions actually performed
my $DRY     = !$OPT{apply};

sub plan {
  my ($msg) = @_;

  # Every persistent change in this script is announced through plan() first,
  # so this is the one place that can guarantee we still hold the advisory lock
  # at the moment we mutate. GET_LOCK is connection-scoped and a silent
  # reconnect drops it, which would let a second run race this one.
  assert_lock_held($msg);

  push @PLAN, $msg;
  $APPLIED++ if !$DRY;
  printf("%s %s\n", $DRY ? '[plan ]' : '[apply]', $msg);
  return;
}

sub note { printf("[info ] %s\n", $_[0]); return; }

sub fatal { die "FATAL: $_[0]\n"; }

########################################################################
# Bootstrap
########################################################################

fatal('--admin-login is required') if !$OPT{'admin-login'};
$OPT{'default-assignee'} //= $OPT{'admin-login'};

my $json_text = do {
  open(my $fh, '<:encoding(UTF-8)', $OPT{state})
    or fatal("cannot read state file '$OPT{state}': $!");
  local $/;
  <$fh>;
};
my $STATE = JSON::PP->new->relaxed->decode($json_text);

# Tells the extension this really is a provisioning run, so that a missing
# cf_category means "not provisioned yet" rather than "someone deleted the
# anchor". Nothing else may claim it.
BEGIN { $ENV{BUGZILLA_ITW_BOOTSTRAP} = 1 }

Bugzilla->usage_mode(USAGE_MODE_CMDLINE);
my $dbh = Bugzilla->dbh;

my $admin = Bugzilla::User->new({name => $OPT{'admin-login'}})
  or fatal("admin login '$OPT{'admin-login'}' does not exist");
fatal("admin '$OPT{'admin-login'}' is disabled") if !$admin->is_enabled;
fatal("'$OPT{'admin-login'}' is not in the 'admin' group")
  if !$admin->in_group('admin');
Bugzilla->set_user($admin);

my $default_assignee = Bugzilla::User->new({name => $OPT{'default-assignee'}})
  or fatal("default assignee '$OPT{'default-assignee'}' does not exist");
fatal("default assignee '$OPT{'default-assignee'}' is disabled")
  if !$default_assignee->is_enabled;

########################################################################
# Static validation of the declared state (before anything is touched)
########################################################################

# Everything below runs against $STATE alone. A typo in the SSOT must be caught
# here, not half way through a mutation.
sub validate_state {
  my %status_names;
  foreach my $spec (@{$STATE->{statuses}}) {
    fatal("duplicate status '$spec->{value}' in expected-state.json")
      if $status_names{$spec->{value}}++;
  }

  my %seen_edge;
  foreach my $edge (@{$STATE->{workflow}}) {
    my $from = $edge->{from};
    my $to   = $edge->{to};
    fatal("workflow edge has no 'to'") if !defined $to || $to eq '';
    fatal("workflow edge from '$from' names unknown status '$to'")
      if !$status_names{$to};
    fatal("workflow edge names unknown source status '$from'")
      if defined $from && $from ne '' && !$status_names{$from};
    my $key = (defined $from ? $from : '') . '|' . $to;
    fatal("duplicate workflow edge '"
        . (defined $from && $from ne '' ? $from : '(new bug)')
        . " -> $to'")
      if $seen_edge{$key}++;
  }

  # Every status must be able to reach the duplicate/move status, or
  # Bugzilla::Status::add_missing_bug_status_transitions() will silently add the
  # row back and our matrix stops matching what is in the database.
  my $dup = $STATE->{params}{duplicate_or_move_bug_status};
  fatal("params.duplicate_or_move_bug_status '$dup' is not a declared status")
    if !$status_names{$dup};
  foreach my $status (sort keys %status_names) {
    next if $status eq $dup;
    fatal("no workflow edge from '$status' to the duplicate status '$dup'")
      if !$seen_edge{"$status|$dup"};
  }

  my %resolution_names;
  foreach my $resolution (@{$STATE->{resolutions}}) {
    fatal("duplicate resolution '$resolution->{value}'")
      if $resolution_names{$resolution->{value}}++;
  }

  foreach my $field (sort keys %{$STATE->{fields}}) {
    my %value_names;
    foreach my $value (@{$STATE->{fields}{$field}{values}}) {
      fatal("duplicate value '$value->{value}' in field $field")
        if $value_names{$value->{value}}++;
    }
  }

  my %type_values;
  foreach my $type (@{$STATE->{issue_types}}) {
    fatal("duplicate entry '$type' in issue_types") if $type_values{$type}++;
  }

  # The type is derived from the product, so every product must map to a
  # declared type and every declared type must own at least one product.
  my %type_used;
  foreach my $product (sort keys %{$STATE->{product_type}}) {
    my $type = $STATE->{product_type}{$product};
    fatal("product_type maps '$product' to unknown issue type '$type'")
      if !$type_values{$type};
    $type_used{$type}++;
  }
  foreach my $type (sort keys %type_values) {
    fatal("no product is mapped to issue type '$type'") if !$type_used{$type};
  }
  foreach my $product (@{$STATE->{products}}) {
    fatal("product '$product->{name}' is not in product_type")
      if !$STATE->{product_type}{$product->{name}};
    fatal("product '$product->{name}' declares issue_type "
        . "'$product->{issue_type}' but product_type says "
        . "'$STATE->{product_type}{$product->{name}}'")
      if $product->{issue_type} ne $STATE->{product_type}{$product->{name}};
  }
  fatal('product_type names a product that is not declared under products')
    if keys %{$STATE->{product_type}} != scalar(@{$STATE->{products}});

  my %status_is_open = map { $_->{value} => $_->{is_open} } @{$STATE->{statuses}};
  foreach my $value (@{$STATE->{fields}{cf_category}{values}}) {
    fatal("category '$value->{value}' names unknown issue type "
        . "'" . ($value->{issue_type} // '') . "'")
      if !$type_values{$value->{issue_type} // ''};
  }
  fatal('cf_category must stay optional: a mandatory select would force a '
      . 'value onto every existing bug')
    if $STATE->{fields}{cf_category}{is_mandatory};

  my $enf = $STATE->{enforcement};
  foreach my $type (sort keys %type_values) {
    fatal("enforcement.initial_status has no entry for '$type'")
      if !$enf->{initial_status}{$type};
    fatal("enforcement.initial_status for '$type' is not a declared status")
      if !$status_names{$enf->{initial_status}{$type}};
    fatal("enforcement.allowed_statuses has no entry for '$type'")
      if !$enf->{allowed_statuses}{$type};
    foreach my $status (@{$enf->{allowed_statuses}{$type}}) {
      fatal("enforcement.allowed_statuses['$type'] names unknown status "
          . "'$status'")
        if !$status_names{$status};
    }
    foreach my $resolution (@{$enf->{allowed_resolutions}{$type}}) {
      fatal("enforcement.allowed_resolutions['$type'] names unknown "
          . "resolution '$resolution'")
        if !$resolution_names{$resolution};
    }
    my $initial = $enf->{initial_status}{$type};
    fatal("enforcement.initial_status['$type'] is not in allowed_statuses")
      if !grep { $_ eq $initial } @{$enf->{allowed_statuses}{$type}};

    # If the entry point has no creation edge, items of this type could never
    # be filed at all - the extension canonicalises to it unconditionally.
    fatal("initial status '$initial' for '$type' has no bug-creation edge")
      if !$seen_edge{"|$initial"};
    fatal("initial status '$initial' for '$type' is a closed status")
      if !$status_is_open{$initial};

    # The category vocabulary is compiled into the extension as well as being
    # expressed as each value's controller, so all three must agree.
    my @declared_categories
      = sort map { $_->{issue_type} eq $type ? $_->{value} : () }
      @{$STATE->{fields}{cf_category}{values}};
    my @enforced_categories = sort @{$enf->{allowed_categories}{$type} || []};
    fatal("enforcement.allowed_categories['$type'] does not match the "
        . 'cf_category values controlled by that type')
      if join('|', @declared_categories) ne join('|', @enforced_categories);
  }

  my %product_names;
  foreach my $product (@{$STATE->{products}}) {
    fatal("duplicate product '$product->{name}'") if $product_names{$product->{name}}++;
    fatal("product '$product->{name}' declares no versions")
      if !@{$product->{versions} || []};
    fatal("product '$product->{name}' default_milestone is not declared")
      if !grep { $_ eq $product->{default_milestone} } @{$product->{milestones}};
    my %component_names;
    foreach my $component (@{$product->{components}}) {
      fatal("duplicate component '$component->{name}' in '$product->{name}'")
        if $component_names{$component->{name}}++;
    }
    my %version_names;
    foreach my $version (@{$product->{versions}}) {
      fatal("duplicate version '$version' in '$product->{name}'")
        if $version_names{$version}++;
    }
    my %milestone_names;
    foreach my $milestone (@{$product->{milestones}}) {
      fatal("duplicate milestone '$milestone' in '$product->{name}'")
        if $milestone_names{$milestone}++;
    }
  }

  note('declared state is internally consistent');
  return;
}

# The extension carries the policy as Perl constants so it cannot be disabled by
# a bad config file. That makes drift between the two the real risk, so assert
# they agree - on every run, before touching anything.
sub validate_extension_agreement {
  fatal('the IssueTypeWorkflow extension is not loaded; check the compose mount')
    if !EXTENSION->can('SPEC_VERSION');

  my $ext_version = EXTENSION->SPEC_VERSION;
  fatal("expected-state.json spec_version '$STATE->{spec_version}' does not "
      . "match the extension's SPEC_VERSION '$ext_version'")
    if $ext_version ne $STATE->{spec_version};

  my $enf = $STATE->{enforcement};

  my $product_type = EXTENSION->PRODUCT_TYPE;
  fatal('extension PRODUCT_TYPE covers a different set of products')
    if keys %$product_type != keys %{$STATE->{product_type}};
  foreach my $product (sort keys %{$STATE->{product_type}}) {
    fatal("extension PRODUCT_TYPE['$product'] is '"
        . ($product_type->{$product} // 'undef')
        . "', expected '$STATE->{product_type}{$product}'")
      if ($product_type->{$product} // '') ne $STATE->{product_type}{$product};
  }

  my $type_by_id = EXTENSION->PRODUCT_TYPE_ID;
  my $want_by_id = $STATE->{product_type_id};
  fatal('extension PRODUCT_TYPE_ID covers a different set of product ids')
    if keys %$type_by_id != keys %$want_by_id;
  foreach my $id (sort keys %$want_by_id) {
    fatal("extension PRODUCT_TYPE_ID[$id] is '"
        . ($type_by_id->{$id} // 'undef') . "', expected '$want_by_id->{$id}'")
      if ($type_by_id->{$id} // '') ne $want_by_id->{$id};
  }

  my $initial = EXTENSION->INITIAL_STATUS;
  foreach my $type (sort keys %{$enf->{initial_status}}) {
    fatal("extension INITIAL_STATUS['$type'] is '"
        . ($initial->{$type} // 'undef')
        . "', expected '$enf->{initial_status}{$type}'")
      if ($initial->{$type} // '') ne $enf->{initial_status}{$type};
  }
  fatal('extension INITIAL_STATUS has types the JSON does not declare')
    if keys %$initial != keys %{$enf->{initial_status}};

  foreach my $pair (
    ['ALLOWED_STATUSES',    $enf->{allowed_statuses}],
    ['ALLOWED_RESOLUTIONS', $enf->{allowed_resolutions}],
    ['ALLOWED_CATEGORIES',  $enf->{allowed_categories}]
    )
  {
    my ($name, $want) = @$pair;
    my $got = EXTENSION->$name;
    fatal("extension $name covers a different set of issue types")
      if keys %$got != keys %$want;
    foreach my $type (sort keys %$want) {
      my $got_list  = join('|', sort keys %{$got->{$type} || {}});
      my $want_list = join('|', sort @{$want->{$type}});
      fatal("extension $name\['$type'] is [$got_list], expected [$want_list]")
        if $got_list ne $want_list;
    }
  }

  my $want_open = {map { $_->{value} => ($_->{is_open} ? 1 : 0) } @{$STATE->{statuses}}};
  my $got_open  = EXTENSION->STATUS_IS_OPEN;
  fatal('extension STATUS_IS_OPEN covers a different set of statuses')
    if keys %$got_open != keys %$want_open;
  foreach my $status (sort keys %$want_open) {
    fatal("extension STATUS_IS_OPEN['$status'] is '"
        . ($got_open->{$status} // 'undef') . "', expected '$want_open->{$status}'")
      if ($got_open->{$status} // -1) != $want_open->{$status};
  }

  my $want_edges = join(';',
    sort map { ($_->{from} // '') . '>' . $_->{to} . '=' . ($_->{require_comment} ? 1 : 0) }
    @{$STATE->{workflow}});
  my $got_edges = join(';',
    sort map { $_->[0] . '>' . $_->[1] . '=' . $_->[2] } @{EXTENSION->WORKFLOW});
  fatal("extension WORKFLOW does not match the declared matrix:\n"
      . "         extension: $got_edges\n         declared:  $want_edges")
    if $got_edges ne $want_edges;

  my $want_codes = $enf->{error_codes}{extension};
  my $got_codes  = EXTENSION->ERROR_CODES;
  fatal('extension ERROR_CODES declares a different set of errors')
    if keys %$got_codes != keys %$want_codes;
  foreach my $error (sort keys %$want_codes) {
    fatal("extension ERROR_CODES['$error'] is '"
        . ($got_codes->{$error} // 'undef')
        . "', expected '$want_codes->{$error}'")
      if ($got_codes->{$error} // -1) != $want_codes->{$error};
  }

  note('extension policy constants agree with expected-state.json');
  return;
}

########################################################################
# Preflight against the live installation
########################################################################

sub check_web_tier_is_down {
  return if $DRY;

  my $target = "$OPT{'web-host'}:$OPT{'web-port'}";
  my $socket = IO::Socket::INET->new(
    PeerHost => $OPT{'web-host'},
    PeerPort => $OPT{'web-port'},
    Proto    => 'tcp',
    Timeout  => 3,
  );
  if (!$socket) {
    my $why = $!;

    # Only an actively refused connection proves nothing is listening. A
    # timeout, an unknown host or an unreachable network means we do not know -
    # and "we do not know" must not pass for "it is safe".
    fatal("cannot tell whether the web tier at $target is down ($why). "
        . 'Check --web-host/--web-port, or pass --allow-live if you are sure.')
      if $why !~ /refused/i;

    note("web tier $target actively refused the connection - good, applying "
        . 'in isolation');
    return;
  }
  close($socket);

  fatal("the web tier is still serving on $OPT{'web-host'}:$OPT{'web-port'}. "
      . 'Run `apachectl stop` in the container first (see README), or pass '
      . '--allow-live to override')
    if !$OPT{'allow-live'};

  note('WARNING: applying while the web tier is live, because --allow-live '
      . 'was given. Enforcement is off part-way through this run.');
  return;
}

my $LOCK_CONNECTION_ID;

sub take_lock {
  # An automatic reconnect would silently drop the advisory lock underneath us,
  # so turn it off where the driver supports it and pin the connection id.
  eval { $dbh->{mysql_auto_reconnect} = 0; 1 };

  my ($got_lock)
    = $dbh->selectrow_array('SELECT GET_LOCK(?, 0)', undef, LOCK_NAME);
  fatal("another setup run holds the '" . LOCK_NAME . "' lock") if !$got_lock;
  ($LOCK_CONNECTION_ID) = $dbh->selectrow_array('SELECT CONNECTION_ID()');
  return;
}

# GET_LOCK is connection-scoped: a silent reconnect drops it. Re-assert
# ownership before each destructive phase rather than assuming we still hold it.
sub assert_lock_held {
  my ($phase) = @_;

  # Checked in dry runs as well: a reconnect there would let a concurrent apply
  # change things underneath us and turn "zero actions" into a false result.
  my ($used) = $dbh->selectrow_array('SELECT IS_USED_LOCK(?)', undef, LOCK_NAME);
  my ($mine) = $dbh->selectrow_array('SELECT CONNECTION_ID()');
  fatal("the database connection was replaced (was $LOCK_CONNECTION_ID, now "
      . (defined $mine ? $mine : 'unknown')
      . "); the advisory lock is gone. Re-run the script.")
    if !defined $mine
    || !defined $LOCK_CONNECTION_ID
    || $mine != $LOCK_CONNECTION_ID;
  fatal("lost the '" . LOCK_NAME . "' lock before $phase; re-run the script")
    if !defined $used || $used != $mine;
  return;
}

my $LOCK_WAS_LOST = 0;

sub release_lock {
  my ($released) = eval { $dbh->selectrow_array('SELECT RELEASE_LOCK(?)', undef, LOCK_NAME) };
  if ($@) {
    $LOCK_WAS_LOST = 1;
    warn "WARNING: could not release the setup lock: $@\n";
    return;
  }

  # 0 or NULL means this connection did not hold the lock when we let go - so
  # the concurrency invariant the whole run relies on was not actually held.
  # That is a failed run, not a warning.
  if (!$released) {
    $LOCK_WAS_LOST = 1;
    warn 'WARNING: RELEASE_LOCK returned '
      . (defined $released ? $released : 'NULL')
      . " - this connection did not hold the setup lock\n";
  }
  return;
}

sub preflight {
  my $version = BUGZILLA_VERSION;
  fatal("expected Bugzilla $STATE->{bugzilla_version}, found $version")
    if $version ne $STATE->{bugzilla_version};

  my %target_status = map { $_->{value} => 1 } @{$STATE->{statuses}};
  my %renamed_from;    # deliberately empty: this design never renames a status

  # Statuses are never renamed. Bugzilla implements a value rename as
  # UPDATE bugs SET <field> = ? (Field/Choice.pm:158), which would rewrite every
  # live bug sitting on it - exactly what we are not allowed to do. Instead the
  # SSOT records which statuses must ALREADY be present; if one is missing, this
  # is not the installation we think it is.
  foreach my $spec (@{$STATE->{statuses}}) {
    next if !$spec->{exists};
    fatal("status '$spec->{value}' is declared as pre-existing but is not "
        . 'present; refusing to create it, because the declared model assumes '
        . 'the stock statuses are the ones live bugs are already using')
      if !Bugzilla::Status->new({name => $spec->{value}});
  }

  my $rows = $dbh->selectall_arrayref(
    'SELECT bug_status, COUNT(*) AS n FROM bugs GROUP BY bug_status',
    {Slice => {}});
  foreach my $row (@$rows) {
    my $status = $row->{bug_status};
    next if $target_status{$status} || $renamed_from{$status};
    fatal("$row->{n} bug(s) use status '$status', which is not in the target "
        . 'model; migrate them first');
  }

  foreach my $spec (@{$STATE->{products}}) {
    my $product = Bugzilla::Product->new({name => $spec->{name}}) or next;
    # classification is absent from Product::UPDATE_COLUMNS, so this can only
    # ever be reported, never converged.
    fatal("product '$spec->{name}' is in classification '"
        . $product->classification->name
        . "', but expected-state.json declares '$spec->{classification}'. "
        . 'Bugzilla cannot move a product between classifications from the '
        . 'object API; fix it in the admin UI.')
      if $product->classification->name ne $spec->{classification};
  }

  my ($engine) = $dbh->selectrow_array(
    'SELECT ENGINE FROM information_schema.TABLES '
      . 'WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?',
    undef, 'status_workflow');
  fatal("status_workflow uses the '" . ($engine // 'unknown')
      . "' engine, which is not transactional. The workflow rewrite's "
      . 'rollback guarantee would not hold; refusing to continue.')
    if !defined $engine || uc($engine) ne 'INNODB';

  # Non-convergeable conditions belong here, before the first mutation, not
  # half way through: an existing custom field of the wrong shape can never be
  # fixed by this script, and neither can a populated product we were asked to
  # remove.
  foreach my $name (sort keys %{$STATE->{fields}}) {
    my $field = Bugzilla::Field->new({name => $name}) or next;
    fatal("field $name exists but is not a custom field") if !$field->custom;
    fatal("field $name exists with type " . $field->type . ', expected '
        . $FIELD_TYPE{$STATE->{fields}{$name}{type}}
        . ' - Bugzilla cannot change a field\'s type')
      if $field->type != $FIELD_TYPE{$STATE->{fields}{$name}{type}};
  }

  if ($OPT{'remove-decommissioned'}) {
    foreach my $name (@{$STATE->{decommission}{products} || []}) {
      my $product = Bugzilla::Product->new({name => $name}) or next;
      my $count = $product->bug_count;
      fatal("refusing to remove product '$name': it holds $count bug(s). "
          . 'Move them to another product (or delete them) first.')
        if $count;
    }
  }

  foreach my $spec (@{$STATE->{products}}) {
    my $product = Bugzilla::Product->new({name => $spec->{name}}) or next;
    my $want = $STATE->{product_type_id}{$product->id};
    fatal("product '$spec->{name}' has id " . $product->id
        . ", which expected-state.json maps to '"
        . (defined $want ? $want : 'nothing')
        . "' rather than '$spec->{issue_type}'. The type is bound to the id as "
        . 'well as the name, so this mismatch must be resolved by hand.')
      if !defined $want || $want ne $spec->{issue_type};
  }

  audit_existing_bugs(\%renamed_from);
  assert_nothing_undeclared();

  note("preflight OK (Bugzilla $version, acting as " . $admin->login . ')');
  return;
}

# Anything present but undeclared is divergence we cannot reason about. This
# runs in PREFLIGHT - before any mutation and in dry runs too - so a predictable
# failure cannot leave a half-provisioned installation, and so a dry run cannot
# print "nothing to do" against an installation the verifier would reject.
#
# Inactive objects count as undeclared: Bugzilla's REST serialisation of generic
# choice values carries no is_active flag, so the verifier cannot tell them
# apart and would fail on them. Deactivating an extra is therefore NOT a valid
# remediation - delete it, or declare it.
sub assert_nothing_undeclared {
  my @problems;

  foreach my $field_name (sort keys %{$STATE->{fields}}) {
    next if !Bugzilla::Field->new({name => $field_name});
    my %declared
      = map { $_->{value} => 1 } @{$STATE->{fields}{$field_name}{values}};
    my @extra
      = grep { $_ ne '' && $_ ne '---' && !$declared{$_} }
      map { $_->name }
      @{Bugzilla::Field::Choice->type($field_name)->match({})};
    push @problems, "$field_name value(s): " . join(', ', sort @extra) if @extra;
  }

  my %declared_status = map { $_->{value} => 1 } @{$STATE->{statuses}};
  my @extra_status = grep { !$declared_status{$_} }
    map { $_->name } Bugzilla::Status->get_all;
  push @problems, 'status(es): ' . join(', ', sort @extra_status)
    if @extra_status;

  my %declared_resolution = map { $_->{value} => 1 } @{$STATE->{resolutions}};
  my @extra_resolution
    = grep { $_ ne '' && !$declared_resolution{$_} }
    map { $_->name }
    @{Bugzilla::Field::Choice->type('resolution')->match({})};
  push @problems, 'resolution(s): ' . join(', ', sort @extra_resolution)
    if @extra_resolution;

  foreach my $spec (@{$STATE->{products}}) {
    my $product = Bugzilla::Product->new({name => $spec->{name}}) or next;
    my %want_component = map { $_->{name} => 1 } @{$spec->{components}};
    my @extra_component
      = grep { !$want_component{$_} } map { $_->name } @{$product->components};
    push @problems, "$spec->{name} component(s): " . join(', ', sort @extra_component)
      if @extra_component;

    my %want_version = map { $_ => 1 } @{$spec->{versions}};
    my @extra_version
      = grep { !$want_version{$_} } map { $_->name } @{$product->versions};
    push @problems, "$spec->{name} version(s): " . join(', ', sort @extra_version)
      if @extra_version;

    my %want_milestone = map { $_ => 1 } @{$spec->{milestones}};
    my @extra_milestone
      = grep { !$want_milestone{$_} } map { $_->name } @{$product->milestones};
    push @problems,
      "$spec->{name} milestone(s): " . join(', ', sort @extra_milestone)
      if @extra_milestone;
  }

  my %known_product
    = map { $_ => 1 } (map { $_->{name} } @{$STATE->{products}}),
    @{$STATE->{decommission}{products} || []};
  my @extra_product
    = grep { !$known_product{$_} } map { $_->name } Bugzilla::Product->get_all;
  push @problems, 'product(s): ' . join(', ', sort @extra_product)
    if @extra_product;

  my @extra_field
    = grep { !$STATE->{fields}{$_} }
    map { $_->name } @{Bugzilla::Field->match({custom => 1})};
  push @problems, 'custom field(s): ' . join(', ', sort @extra_field)
    if @extra_field;

  fatal("the installation contains objects expected-state.json does not "
      . "declare:\n         " . join("\n         ", @problems)
      . "\n         Declare them, or delete them (deactivating is not enough - "
      . 'REST cannot report it).')
    if @problems;

  note('nothing undeclared is present');
  return;
}

# The extension only validates fields as they change, so it can never repair an
# item that is already inconsistent. Refuse to declare the model in force while
# any existing item violates it.
sub audit_existing_bugs {
  my ($renamed_from) = @_;

  my ($bug_count) = $dbh->selectrow_array('SELECT COUNT(*) FROM bugs');
  return note('no existing bugs to audit') if !$bug_count;

  # cf_category is OPTIONAL, so creating it does not write to a single existing
  # row and an empty value is valid forever. That is the whole reason the type
  # is derived from the product instead of stored per bug.
  my $has_category = Bugzilla::Field->new({name => 'cf_category'}) ? 1 : 0;

  my $select
    = 'SELECT b.bug_id, b.bug_status, b.resolution, p.name AS product'
    . ($has_category ? ', b.cf_category' : '')
    . ' FROM bugs b JOIN products p ON p.id = b.product_id';
  my $bugs = $dbh->selectall_arrayref($select, {Slice => {}});

  my $enf = $STATE->{enforcement};
  my %category_type = map { $_->{value} => $_->{issue_type} }
    @{$STATE->{fields}{cf_category}{values}};

  my @bad;
  foreach my $bug (@$bugs) {
    my $type = $STATE->{product_type}{$bug->{product}};
    if (!defined $type) {
      push @bad, "bug $bug->{bug_id}: product '$bug->{product}' has no declared type";
      next;
    }

    my $status = $bug->{bug_status};
    push @bad, "bug $bug->{bug_id}: $type on status '$status'"
      if !grep { $_ eq $status } @{$enf->{allowed_statuses}{$type}};

    my $resolution = $bug->{resolution};
    push @bad, "bug $bug->{bug_id}: $type with resolution '$resolution'"
      if defined $resolution
      && $resolution ne ''
      && !grep { $_ eq $resolution } @{$enf->{allowed_resolutions}{$type}};

    # An empty category is always valid - that is what leaves existing bugs
    # untouched. Only a WRONG one is a problem.
    if ($has_category) {
      my $category = $bug->{cf_category};
      if (defined $category && $category ne '' && $category ne '---') {
        push @bad, "bug $bug->{bug_id}: $type with category '$category'"
          if ($category_type{$category} // '') ne $type;
      }
    }
  }

  if (@bad) {
    my @show = @bad > 20 ? (@bad[0 .. 19], '...') : @bad;
    fatal("existing items are inconsistent with the declared model:\n         "
        . join("\n         ", @show));
  }

  note("$bug_count existing bug(s) audited against their product's type: all "
      . 'consistent, and none will be written to');
  return;
}

# Bugzilla caches field/choice/status objects per request and in memcached. We
# are a long-lived process mutating exactly those objects, so drop the caches
# between phases. A failure here is fatal: silently continuing on a stale cache
# is how a run reports success against a database it never actually converged.
sub flush_caches {
  my $cache = Bugzilla->request_cache;
  delete $cache->{fields};
  delete $cache->{active_custom_fields};
  delete $cache->{status_bug_state_open};
  delete $cache->{itw_state};
  foreach my $key (keys %$cache) {
    delete $cache->{$key}
      if $key
      =~ /^Bugzilla::(Field|Field::Choice|Status|Product|Component|Version|Milestone)/;
  }
  my $result = eval { Bugzilla->memcached->clear_all };
  fatal('could not flush memcached: ' . $@) if $@;
  fatal('memcached clear_all reported failure') if !$result;
  return;
}

########################################################################
# Fields and field values
########################################################################

sub ensure_field {
  my ($name, $spec) = @_;

  my $type = $FIELD_TYPE{$spec->{type}}
    or fatal("unsupported field type '$spec->{type}' for $name");

  my $field = Bugzilla::Field->new({name => $name});

  if (!$field) {
    plan("create custom field $name ($spec->{type}, mandatory="
        . ($spec->{is_mandatory} ? 1 : 0) . ')');
    return if $DRY;

    $field = Bugzilla::Field->create({
      name         => $name,
      description  => $spec->{description},
      type         => $type,
      custom       => 1,
      enter_bug    => $spec->{enter_bug}    ? 1 : 0,
      buglist      => $spec->{buglist}      ? 1 : 0,
      is_mandatory => $spec->{is_mandatory} ? 1 : 0,
      sortkey      => $spec->{sortkey},
      obsolete     => 0,
    });
    flush_caches();
    return $field;
  }

  fatal("field $name exists with type " . $field->type . ", expected $type")
    if $field->type != $type;
  fatal("field $name exists but is not a custom field")
    if !$field->custom;

  my %want = (
    description  => $spec->{description},
    enter_bug    => $spec->{enter_bug}    ? 1 : 0,
    buglist      => $spec->{buglist}      ? 1 : 0,
    is_mandatory => $spec->{is_mandatory} ? 1 : 0,
    sortkey      => $spec->{sortkey},
    obsolete     => 0,
  );
  my $changed = 0;
  foreach my $attr (sort keys %want) {
    my $current = $field->$attr;
    $current = defined($current) ? $current : '';
    next if "$current" eq "$want{$attr}";
    plan("field $name: set $attr from '$current' to '$want{$attr}'");
    next if $DRY;
    $field->set($attr, $want{$attr});
    $changed = 1;
  }
  if ($changed && !$DRY) { $field->update; flush_caches(); }

  return $field;
}

# value_field/visibility_field must be wired only after BOTH fields exist.
sub ensure_field_controls {
  my ($name, $spec) = @_;

  my $field = Bugzilla::Field->new({name => $name});
  if (!$field) {
    fatal("field $name does not exist") if !$DRY;
    plan("field $name: wire value_field/visibility control (after the field "
        . 'is created)');
    return;
  }
  my $changed = 0;

  my $want_value_field = $spec->{value_field} // '';
  my $have_value_field = $field->value_field ? $field->value_field->name : '';
  if ($have_value_field ne $want_value_field) {
    plan("field $name: set value_field from '$have_value_field' to "
        . "'" . ($want_value_field || '(none)') . "'");
    if (!$DRY) {
      my $id;
      if ($want_value_field) {
        my $controller = Bugzilla::Field->new({name => $want_value_field})
          or fatal("value_field '$want_value_field' for $name does not exist");
        $id = $controller->id;
      }
      $field->set('value_field_id', $id);
      $changed = 1;
    }
  }

  my $want_vis_field = $spec->{visibility_field} // '';
  my $have_vis_field = $field->visibility_field ? $field->visibility_field->name : '';
  if ($have_vis_field ne $want_vis_field) {
    plan("field $name: set visibility_field from '$have_vis_field' to "
        . "'" . ($want_vis_field || '(none)') . "'");
    if (!$DRY) {
      my $id;
      if ($want_vis_field) {
        my $controller = Bugzilla::Field->new({name => $want_vis_field})
          or fatal("visibility_field '$want_vis_field' for $name does not exist");
        $id = $controller->id;
      }
      $field->set('visibility_field_id', $id);
      $changed = 1;
    }
  }

  if (!$want_vis_field) {
    my @stale = map { $_->name } @{$field->visibility_values || []};
    if (@stale) {
      plan("field $name: clear stale visibility_values [" . join(', ', sort @stale) . ']');
      if (!$DRY) { $field->set('visibility_values', []); $changed = 1; }
    }
  }
  else {
    my @want = sort @{$spec->{visibility_values} || []};
    my @have = sort map { $_->name } @{$field->visibility_values || []};
    if (join('|', @want) ne join('|', @have)) {
      plan("field $name: set visibility_values to [" . join(', ', @want) . ']');
      if (!$DRY) {
        my @ids;
        foreach my $value (@want) {
          my $choice
            = Bugzilla::Field::Choice->type($want_vis_field)->new({name => $value})
            or fatal("visibility value '$value' of '$want_vis_field' "
              . 'does not exist');
          push @ids, $choice->id;
        }
        $field->set('visibility_values', \@ids);
        $changed = 1;
      }
    }
  }

  if ($changed && !$DRY) { $field->update; flush_caches(); }
  return;
}

sub ensure_choice {
  my ($field_name, $spec) = @_;

  # On a first dry run the field itself does not exist yet, so there is nothing
  # to inspect - just report what would be created.
  my $field = Bugzilla::Field->new({name => $field_name});
  if (!$field) {
    fatal("field $field_name does not exist") if !$DRY;
    plan("create $field_name value '$spec->{value}' (after the field is created)");
    return;
  }

  # No choice in this model carries its own controller: cf_category is a flat
  # vocabulary the extension checks against the product-derived type, and
  # cf_environment is controlled at FIELD level by product.
  my $controller_id;

  my $class  = Bugzilla::Field::Choice->type($field_name);
  my $choice = $class->new({name => $spec->{value}});

  if (!$choice) {
    plan("create $field_name value '$spec->{value}'");
    return if $DRY;
    $class->create({
      value               => $spec->{value},
      sortkey             => $spec->{sortkey},
      isactive            => 1,
      visibility_value_id => $controller_id,
    });
    return;
  }

  my $changed = 0;
  if (($choice->sortkey // -1) != $spec->{sortkey}) {
    plan("$field_name value '$spec->{value}': sortkey "
        . ($choice->sortkey // 'undef') . " -> $spec->{sortkey}");
    if (!$DRY) { $choice->set('sortkey', $spec->{sortkey}); $changed = 1; }
  }
  if (!$choice->is_active) {
    plan("$field_name value '$spec->{value}': reactivate");
    if (!$DRY) { $choice->set('isactive', 1); $changed = 1; }
  }
  # Clear any controller a previous model left on the value.
  my $have_controller = $choice->visibility_value ? $choice->visibility_value->id : 0;
  if ($have_controller) {
    plan("$field_name value '$spec->{value}': clear its value controller");
    if (!$DRY) { $choice->set('visibility_value_id', undef); $changed = 1; }
  }
  $choice->update if $changed && !$DRY;
  return;
}

########################################################################
# Statuses, resolutions, workflow
########################################################################

sub ensure_statuses {
  foreach my $spec (@{$STATE->{statuses}}) {
    my $status = Bugzilla::Status->new({name => $spec->{value}});

    if (!$status) {
      fatal("status '$spec->{value}' should already exist")
        if $spec->{exists};
      plan("create status '$spec->{value}' (is_open="
          . ($spec->{is_open} ? 1 : 0) . ')');
      next if $DRY;

      # is_open is settable only at create time: Bugzilla::Status inherits
      # Bugzilla::Field::Choice::UPDATE_COLUMNS, which does not include it.
      Bugzilla::Status->create({
        value    => $spec->{value},
        sortkey  => $spec->{sortkey},
        isactive => 1,
        is_open  => $spec->{is_open} ? 1 : 0,
      });
      next;
    }

    my $changed = 0;
    if (($status->sortkey // -1) != $spec->{sortkey}) {
      plan("status '$spec->{value}': sortkey "
          . ($status->sortkey // 'undef') . " -> $spec->{sortkey}");
      if (!$DRY) { $status->set('sortkey', $spec->{sortkey}); $changed = 1; }
    }
    if (!$status->is_active) {
      plan("status '$spec->{value}': reactivate");
      if (!$DRY) { $status->set('isactive', 1); $changed = 1; }
    }
    $status->update if $changed && !$DRY;

    my $want_open = $spec->{is_open} ? 1 : 0;
    if (($status->is_open ? 1 : 0) != $want_open) {
      plan("status '$spec->{value}': is_open " . ($status->is_open ? 1 : 0)
          . " -> $want_open (direct SQL; not an updatable column)");
      if (!$DRY) {
        $dbh->do('UPDATE bug_status SET is_open = ? WHERE id = ?',
          undef, $want_open, $status->id);
      }
    }
  }

  flush_caches() if !$DRY;
  return;
}

sub ensure_resolutions {
  ensure_choice('resolution', $_) foreach @{$STATE->{resolutions}};
  flush_caches() if !$DRY;
  return;
}

sub rewrite_workflow {
  my %id_of;
  foreach my $spec (@{$STATE->{statuses}}) {
    my $status = Bugzilla::Status->new({name => $spec->{value}});
    if (!$status) {
      if ($DRY) {
        plan('rewrite status_workflow to the declared '
            . scalar(@{$STATE->{workflow}})
            . '-edge matrix (exact diff unavailable until the statuses exist)');
        return;
      }
      fatal("status '$spec->{value}' missing while rewriting the workflow");
    }
    $id_of{$spec->{value}} = $status->id;
  }

  # validate_state() already proved every name resolves, so an undef here would
  # be a programming error rather than bad input - but the creation row is the
  # one thing that must never be produced by accident.
  my @want;
  foreach my $edge (@{$STATE->{workflow}}) {
    my $from = $edge->{from};
    my $old_id;
    if (defined $from && $from ne '') {
      $old_id = $id_of{$from} or fatal("unknown workflow source '$from'");
    }
    my $new_id = $id_of{$edge->{to}} or fatal("unknown workflow target '$edge->{to}'");
    push @want, [$old_id, $new_id, ($edge->{require_comment} ? 1 : 0)];
  }

  my $key = sub {
    my ($row) = @_;
    return join('|', defined $row->[0] ? $row->[0] : 'NULL', $row->[1], $row->[2]);
  };

  my $have = $dbh->selectall_arrayref(
    'SELECT old_status, new_status, require_comment FROM status_workflow');

  # Compare as multisets. MariaDB's UNIQUE(old_status, new_status) does not
  # constrain rows whose old_status is NULL, so duplicate creation rows are
  # possible and must not be collapsed away by the comparison.
  my (%have_count, %want_count);
  $have_count{$key->($_)}++ foreach @$have;
  $want_count{$key->($_)}++ foreach @want;

  my $identical = (keys(%have_count) == keys(%want_count))
    && !grep { ($have_count{$_} // 0) != $want_count{$_} } keys %want_count;

  if ($identical) {
    note('status_workflow already matches the declared matrix');
    return;
  }

  plan('rewrite status_workflow: ' . scalar(@$have) . ' row(s) -> '
      . scalar(@want) . ' row(s)');
  return if $DRY;

  assert_lock_held('the status_workflow rewrite');

  $dbh->bz_start_transaction();
  my $ok = eval {
    $dbh->do('DELETE FROM status_workflow');
    my $sth = $dbh->prepare(
      'INSERT INTO status_workflow (old_status, new_status, require_comment) '
        . 'VALUES (?, ?, ?)');
    $sth->execute(@$_) foreach @want;

    # Read back inside the transaction: a silently-wrong workflow is the single
    # most damaging change this script can make.
    my $check = $dbh->selectall_arrayref(
      'SELECT old_status, new_status, require_comment FROM status_workflow');
    my %check_count;
    $check_count{$key->($_)}++ foreach @$check;
    die "read-back mismatch\n"
      if keys(%check_count) != keys(%want_count)
      || grep { ($check_count{$_} // 0) != $want_count{$_} } keys %want_count;
    1;
  };
  if (!$ok) {
    my $err        = $@ || "unknown error\n";
    my $rolled_back = eval { $dbh->bz_rollback_transaction(); 1 };
    fatal("status_workflow rewrite failed AND the rollback failed ($@). "
        . "The matrix may be empty or partial - restore from backup before "
        . "restarting the web tier. Original error: $err")
      if !$rolled_back;
    fatal("status_workflow rewrite failed and was rolled back: $err");
  }
  $dbh->bz_commit_transaction();
  flush_caches();
  return;
}

########################################################################
# Products and components
########################################################################

sub ensure_product {
  my ($spec) = @_;
  my $product = Bugzilla::Product->new({name => $spec->{name}});

  if (!$product) {
    plan("create product '$spec->{name}' with "
        . scalar(@{$spec->{components}}) . ' component(s)');
    return if $DRY;
    $product = Bugzilla::Product->create({
      name               => $spec->{name},
      description        => $spec->{description},
      classification     => $spec->{classification},
      version            => $spec->{versions}[0],
      defaultmilestone   => $spec->{default_milestone},
      isactive           => $spec->{is_active} ? 1 : 0,
      allows_unconfirmed => $spec->{allows_unconfirmed} ? 1 : 0,
      create_series      => 1,
    });
  }
  else {
    my %want = (
      description        => $spec->{description},
      isactive           => $spec->{is_active} ? 1 : 0,
      allows_unconfirmed => $spec->{allows_unconfirmed} ? 1 : 0,
      defaultmilestone   => $spec->{default_milestone},
    );
    my %getter = (
      isactive           => 'is_active',
      allows_unconfirmed => 'allows_unconfirmed',
      defaultmilestone   => 'default_milestone',
      description        => 'description',
    );
    my $changed = 0;
    foreach my $attr (sort keys %want) {
      my $getter  = $getter{$attr};
      my $current = $product->$getter;
      $current = defined($current) ? $current : '';
      next if "$current" eq "$want{$attr}";
      plan("product '$spec->{name}': set $attr from '$current' to '$want{$attr}'");
      next if $DRY;
      $product->set($attr, $want{$attr});
      $changed = 1;
    }
    $product->update if $changed && !$DRY;
  }

  if ($DRY && !$product) {
    plan("product '$spec->{name}': create version(s) "
        . join(', ', @{$spec->{versions}}) . ", milestone(s) "
        . join(', ', @{$spec->{milestones}}) . ', and '
        . scalar(@{$spec->{components}})
        . ' component(s) assigned to ' . $default_assignee->login);
    return;
  }

  ensure_versions($product, $spec);
  ensure_milestones($product, $spec);
  ensure_components($product, $spec);
  return;
}

sub ensure_versions {
  my ($product, $spec) = @_;
  my %have = map { $_->name => $_ } @{$product->versions};

  foreach my $name (@{$spec->{versions}}) {
    if (!$have{$name}) {
      plan("product '$spec->{name}': create version '$name'");
      Bugzilla::Version->create({product => $product, value => $name})
        if !$DRY;
      next;
    }
    if (!$have{$name}->is_active) {
      plan("product '$spec->{name}': reactivate version '$name'");
      if (!$DRY) { $have{$name}->set('isactive', 1); $have{$name}->update; }
    }
  }

  return;
}

sub ensure_milestones {
  my ($product, $spec) = @_;
  my %have = map { $_->name => $_ } @{$product->milestones};

  foreach my $name (@{$spec->{milestones}}) {
    if (!$have{$name}) {
      plan("product '$spec->{name}': create milestone '$name'");
      Bugzilla::Milestone->create(
        {product => $product, value => $name, sortkey => 0})
        if !$DRY;
      next;
    }
    if (!$have{$name}->is_active) {
      plan("product '$spec->{name}': reactivate milestone '$name'");
      if (!$DRY) { $have{$name}->set('isactive', 1); $have{$name}->update; }
    }
  }

  return;
}

sub ensure_components {
  my ($product, $spec) = @_;
  my %have = map { $_->name => $_ } @{$product->components};

  foreach my $comp (@{$spec->{components}}) {
    my $existing = $have{$comp->{name}};
    if (!$existing) {
      plan("product '$spec->{name}': create component '$comp->{name}' "
          . '(default assignee ' . $default_assignee->login . ')');
      next if $DRY;
      Bugzilla::Component->create({
        product       => $product,
        name          => $comp->{name},
        description   => $comp->{description},
        initialowner  => $default_assignee->login,
        isactive      => 1,
        create_series => 1,
      });
      next;
    }
    my $changed = 0;
    if ($existing->default_assignee->login ne $default_assignee->login) {
      plan("product '$spec->{name}': component '$comp->{name}' default "
          . 'assignee ' . $existing->default_assignee->login . ' -> '
          . $default_assignee->login);
      if (!$DRY) {
        $existing->set('initialowner', $default_assignee->login);
        $changed = 1;
      }
    }
    if ($existing->description ne $comp->{description}) {
      plan("product '$spec->{name}': component '$comp->{name}' description updated");
      if (!$DRY) { $existing->set('description', $comp->{description}); $changed = 1; }
    }
    if (!$existing->is_active) {
      plan("product '$spec->{name}': reactivate component '$comp->{name}'");
      if (!$DRY) { $existing->set('isactive', 1); $changed = 1; }
    }
    $existing->update if $changed && !$DRY;
  }

  return;
}

# Decommissioning is opt-in and never happens as a side effect of a normal
# apply: the runbook retires TestProduct only after verification has passed.
sub decommission_products {
  foreach my $name (@{$STATE->{decommission}{products} || []}) {
    my $product = Bugzilla::Product->new({name => $name});
    if (!$product) {
      note("product '$name' is already gone");
      next;
    }

    if (!$OPT{'remove-decommissioned'}) {
      note("product '$name' still exists; re-run with --remove-decommissioned "
          . 'after verification to retire it');
      next;
    }

    my $count = $product->bug_count;
    # Deliberately an abort, not a deactivation: a half-retired product that
    # still holds items is worse than an obvious failure. Closing the items
    # does not help - bug_count counts them regardless - so they have to be
    # moved to another product or deleted.
    fatal("refusing to remove product '$name': it holds $count bug(s). "
        . 'Move them to another product (or delete them) first.')
      if $count;

    plan("delete product '$name' (0 bugs)");
    next if $DRY;
    assert_lock_held("removing product '$name'");
    $product->remove_from_db;
  }
  return;
}

########################################################################
# Post-apply self-test
########################################################################

# Proves the property everything else rests on: that damaging the model really
# does put the extension into its 'broken' (refuse-everything) state, rather
# than silently reverting to no policy at all.
#
# It does this for real - by deleting the bug-creation rows - inside a
# transaction it then rolls back. A regression in model_is_complete() is
# otherwise invisible until the day it matters.
sub self_test_fail_closed {
  return if $DRY;

  fatal('the IssueTypeWorkflow extension does not expose model_is_complete()')
    if !EXTENSION->can('model_is_complete');

  flush_caches();
  fatal('self-test: the model is NOT complete after a successful apply')
    if !EXTENSION->model_is_complete;

  assert_lock_held('the fail-closed self-test');
  $dbh->bz_start_transaction();
  my $tripped = eval {
    $dbh->do('DELETE FROM status_workflow WHERE old_status IS NULL');
    flush_caches();

    # Assert the STATE the hooks branch on, not just the predicate behind it:
    # a regression that disconnected the two would otherwise pass this test.
    my $state = Bugzilla::Extension::IssueTypeWorkflow::_enforcement_state();
    (!EXTENSION->model_is_complete && $state eq 'broken') ? 1 : 0;
  };
  my $err = $@;
  eval { $dbh->bz_rollback_transaction(); 1 }
    or fatal("self-test rollback FAILED - the workflow may be damaged: $@");
  flush_caches();

  fatal("self-test errored: $err") if $err;
  fatal('self-test: deleting the bug-creation rows did NOT put the extension '
      . "into its 'broken' state. Enforcement would silently stop instead of "
      . 'blocking. Refusing to declare this installation provisioned.')
    if !$tripped;

  fatal('self-test: the model is not complete again after rollback')
    if !EXTENSION->model_is_complete;

  note('fail-closed self-test passed (damage trips it; rollback restored it)');
  return;
}

########################################################################
# Params
########################################################################

sub ensure_params {
  my $params  = $STATE->{params} || {};
  my $changed = 0;
  foreach my $name (sort keys %$params) {
    my $want    = $params->{$name};
    my $current = Bugzilla->params->{$name};
    $current = defined($current) ? $current : '';
    next if "$current" eq "$want";
    plan("param $name: '$current' -> '$want'");
    next if $DRY;
    SetParam($name, $want);
    $changed = 1;
  }
  if ($changed && !$DRY) {
    assert_lock_held('writing params');
    write_params();
  }
  return;
}

########################################################################
# Main
########################################################################

validate_state();
validate_extension_agreement();
check_web_tier_is_down();
take_lock();

my $ok = eval {
  preflight();

  note($DRY
    ? 'DRY RUN - no persistent change will be made. Re-run with --apply.'
    : 'APPLYING changes.');

  # cf_environment is controlled by 'product', which already exists, so the
  # fields can be created and wired in one pass.
  foreach my $name (qw(cf_category cf_environment)) {
    ensure_field($name, $STATE->{fields}{$name});
  }
  flush_caches() if !$DRY;

  foreach my $name (qw(cf_category cf_environment)) {
    ensure_field_controls($name, $STATE->{fields}{$name});
  }
  flush_caches() if !$DRY;

  foreach my $name (qw(cf_category cf_environment)) {
    ensure_choice($name, $_) foreach @{$STATE->{fields}{$name}{values}};
  }
  flush_caches() if !$DRY;

  ensure_statuses();
  ensure_resolutions();
  ensure_product($_) foreach @{$STATE->{products}};

  # Last, because Bugzilla::Status::create() calls
  # add_missing_bug_status_transitions() and would otherwise leave rows behind.
  rewrite_workflow();

  decommission_products();
  flush_caches() if !$DRY;

  # Re-audit AFTER the DDL: creating a mandatory select field over existing rows
  # would have given them the unset sentinel, and declaring the model complete
  # over that would lock those items out of every guarded edit.
  audit_existing_bugs({}) if !$DRY;

  ensure_params();

  # Only meaningful once everything above is in place.
  self_test_fail_closed() if !$OPT{'skip-self-test'};

  flush_caches() if !$DRY;
  1;
};
my $err = $@;

release_lock();
die $err if !$ok;
fatal('the setup lock was not held when the run finished; another process may '
    . 'have changed the configuration concurrently. Re-run the dry run and '
    . 'compare before trusting this result.')
  if $LOCK_WAS_LOST;

if (!@PLAN) {
  print "\nNo changes - the installation already matches expected-state.json.\n";
  print "(The run still cleared caches"
    . ($DRY ? '' : ' and ran the fail-closed self-test, which briefly damages '
      . 'and rolls back the workflow inside a transaction')
    . ".)\n";
  exit 0;
}

printf("\n%s: %d action(s) %s\n",
  $DRY ? 'DRY RUN' : 'APPLIED', scalar(@PLAN), $DRY ? 'planned' : 'performed');
printf("%d action(s) executed against the database.\n", $APPLIED) if !$DRY;
print "\nNEXT: restart the Bugzilla web container, then run verify-projects.py.\n"
  if !$DRY;

exit 0;
