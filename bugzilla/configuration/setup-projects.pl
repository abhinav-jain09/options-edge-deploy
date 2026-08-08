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
use constant EXTENSION  => 'Bugzilla::Extension::IssueTypeWorkflow';

my %OPT = (
  state                   => 'expected-state.json',
  'admin-login'           => undef,
  'default-assignee'      => undef,
  apply                   => 0,
  'remove-decommissioned' => 0,
  'web-host'              => 'localhost',
  'web-port'              => 80,
  'allow-live'            => 0,
  help                    => 0,
);

GetOptions(\%OPT,
  'state=s', 'admin-login=s', 'default-assignee=s', 'apply!',
  'remove-decommissioned!', 'web-host=s', 'web-port=i', 'allow-live!', 'help!')
  or die "FATAL: bad options. Try --help.\n";

if ($OPT{help}) {
  print <<'USAGE';
setup-projects.pl [options]

  --state FILE               expected-state.json (default: ./expected-state.json)
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
  --help                     this text
USAGE
  exit 0;
}

my @PLAN;                 # every action, planned or performed
my $APPLIED = 0;          # actions actually performed
my $DRY     = !$OPT{apply};

sub plan {
  my ($msg) = @_;
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

  my %resolution_names = map { $_->{value} => 1 } @{$STATE->{resolutions}};
  foreach my $field (sort keys %{$STATE->{fields}}) {
    my %value_names;
    foreach my $value (@{$STATE->{fields}{$field}{values}}) {
      fatal("duplicate value '$value->{value}' in field $field")
        if $value_names{$value->{value}}++;
    }
  }

  my %type_values
    = map { $_->{value} => 1 } @{$STATE->{fields}{cf_issue_type}{values}};
  foreach my $type (@{$STATE->{issue_types}}) {
    fatal("issue_types lists '$type', which is not a cf_issue_type value")
      if !$type_values{$type};
  }
  foreach my $value (@{$STATE->{fields}{cf_category}{values}}) {
    fatal("category '$value->{value}' is controlled by unknown type "
        . "'$value->{controlled_by}'")
      if !$type_values{$value->{controlled_by} // ''};
  }

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
    fatal("enforcement.initial_status['$type'] is not in allowed_statuses")
      if !grep { $_ eq $enf->{initial_status}{$type} }
      @{$enf->{allowed_statuses}{$type}};
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
    ['ALLOWED_RESOLUTIONS', $enf->{allowed_resolutions}]
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

  my $socket = IO::Socket::INET->new(
    PeerHost => $OPT{'web-host'},
    PeerPort => $OPT{'web-port'},
    Proto    => 'tcp',
    Timeout  => 3,
  );
  if (!$socket) {
    note("web tier $OPT{'web-host'}:$OPT{'web-port'} is not accepting "
        . 'connections - good, applying in isolation');
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

sub take_lock {
  my ($got_lock)
    = $dbh->selectrow_array('SELECT GET_LOCK(?, 0)', undef, LOCK_NAME);
  fatal("another setup run holds the '" . LOCK_NAME . "' lock") if !$got_lock;
  return;
}

# GET_LOCK is connection-scoped: a silent reconnect drops it. Re-assert
# ownership before each destructive phase rather than assuming we still hold it.
sub assert_lock_held {
  my ($phase) = @_;
  return if $DRY;
  my ($used) = $dbh->selectrow_array('SELECT IS_USED_LOCK(?)', undef, LOCK_NAME);
  my ($mine) = $dbh->selectrow_array('SELECT CONNECTION_ID()');
  fatal("lost the '" . LOCK_NAME . "' lock before $phase "
      . '(database reconnect?); re-run the script')
    if !defined $used || $used != $mine;
  return;
}

sub release_lock {
  eval { $dbh->selectrow_array('SELECT RELEASE_LOCK(?)', undef, LOCK_NAME); 1 }
    or warn "WARNING: could not release the setup lock: $@";
  return;
}

sub preflight {
  my $version = BUGZILLA_VERSION;
  fatal("expected Bugzilla $STATE->{bugzilla_version}, found $version")
    if $version ne $STATE->{bugzilla_version};

  my %target_status = map { $_->{value} => 1 } @{$STATE->{statuses}};
  my %renamed_from
    = map { $_->{rename_from} => $_->{value} }
    grep  { $_->{rename_from} } @{$STATE->{statuses}};

  # A rename can only be applied while the old name is the ONLY one present.
  # If both exist, bugs on the old status would be silently left on a status the
  # workflow rewrite is about to drop.
  foreach my $old (sort keys %renamed_from) {
    next if !Bugzilla::Status->new({name => $old});
    fatal("both '$old' and its rename target '$renamed_from{$old}' exist; "
        . 'migrate the bugs and remove one before re-running')
      if Bugzilla::Status->new({name => $renamed_from{$old}});
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

  audit_existing_bugs(\%renamed_from);

  note("preflight OK (Bugzilla $version, acting as " . $admin->login . ')');
  return;
}

# The extension only validates fields as they change, so it can never repair an
# item that is already inconsistent. Refuse to declare the model in force while
# any existing item violates it.
sub audit_existing_bugs {
  my ($renamed_from) = @_;

  my $type_field = Bugzilla::Field->new({name => 'cf_issue_type'});
  return note('no cf_issue_type field yet: no existing items to audit')
    if !$type_field;

  my $has_category = Bugzilla::Field->new({name => 'cf_category'}) ? 1 : 0;
  my $select
    = 'SELECT bug_id, bug_status, resolution, cf_issue_type'
    . ($has_category ? ', cf_category' : '')
    . ' FROM bugs';
  my $bugs = $dbh->selectall_arrayref($select, {Slice => {}});
  return note('no existing bugs to audit') if !@$bugs;

  my $enf = $STATE->{enforcement};
  my %category_owner = map { $_->{value} => $_->{controlled_by} }
    @{$STATE->{fields}{cf_category}{values}};

  my @bad;
  foreach my $bug (@$bugs) {
    my $type = $bug->{cf_issue_type};
    if (!defined $type || $type eq '' || $type eq '---') {
      push @bad, "bug $bug->{bug_id}: no issue type";
      next;
    }
    if (!$enf->{allowed_statuses}{$type}) {
      push @bad, "bug $bug->{bug_id}: unknown issue type '$type'";
      next;
    }

    my $status = $renamed_from->{$bug->{bug_status}} // $bug->{bug_status};
    push @bad, "bug $bug->{bug_id}: $type on status '$status'"
      if !grep { $_ eq $status } @{$enf->{allowed_statuses}{$type}};

    my $resolution = $bug->{resolution};
    push @bad, "bug $bug->{bug_id}: $type with resolution '$resolution'"
      if defined $resolution
      && $resolution ne ''
      && !grep { $_ eq $resolution } @{$enf->{allowed_resolutions}{$type}};

    if ($has_category) {
      my $category = $bug->{cf_category};
      if (!defined $category || $category eq '' || $category eq '---') {
        push @bad, "bug $bug->{bug_id}: no category";
      }
      elsif (($category_owner{$category} // '') ne $type) {
        push @bad, "bug $bug->{bug_id}: $type with category '$category'";
      }
    }
  }

  fatal("existing items violate the target model and cannot be repaired by "
      . "this script:\n         " . join("\n         ", @bad[0 .. 19]) . "\n         ...")
    if @bad > 20;
  fatal("existing items violate the target model and cannot be repaired by "
      . "this script:\n         " . join("\n         ", @bad))
    if @bad;

  note(scalar(@$bugs) . ' existing bug(s) audited: all consistent with the model');
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
  delete $cache->{itw_provisioned};
  foreach my $key (keys %$cache) {
    delete $cache->{$key}
      if $key
      =~ /^Bugzilla::(Field|Field::Choice|Status|Product|Component|Version|Milestone)/;
  }
  eval { Bugzilla->memcached->clear_all; 1 }
    or fatal("could not flush memcached: $@");
  return;
}

########################################################################
# Fields and field values
########################################################################

my %FIELD_TYPE = (single_select => FIELD_TYPE_SINGLE_SELECT,);

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
  my $field = Bugzilla::Field->new({name => $name}) or return;
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

  if ($want_vis_field) {
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

  my $controller_id;
  if (my $by = $spec->{controlled_by}) {
    my $vf = $field->value_field ? $field->value_field->name : undef;
    if (!$vf) {
      fatal("$field_name has no value_field, cannot control '$spec->{value}'")
        if !$DRY;
      plan("create $field_name value '$spec->{value}' (controlled by $by, "
          . 'after value_field is wired)');
      return;
    }
    my $controller = Bugzilla::Field::Choice->type($vf)->new({name => $by})
      or fatal("controlling value '$by' of '$vf' does not exist");
    $controller_id = $controller->id;
  }

  my $class  = Bugzilla::Field::Choice->type($field_name);
  my $choice = $class->new({name => $spec->{value}});

  if (!$choice) {
    plan("create $field_name value '$spec->{value}'"
        . ($spec->{controlled_by} ? " (controlled by $spec->{controlled_by})" : ''));
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
  my $have_controller = $choice->visibility_value ? $choice->visibility_value->id : 0;
  if ($have_controller != ($controller_id // 0)) {
    plan("$field_name value '$spec->{value}': controlled_by -> "
        . ($spec->{controlled_by} // '(none)'));
    if (!$DRY) { $choice->set('visibility_value_id', $controller_id); $changed = 1; }
  }
  $choice->update if $changed && !$DRY;
  return;
}

# Anything active in the installation that the SSOT does not declare is a
# divergence we cannot reason about - stop rather than pretend we converged.
sub assert_no_undeclared_choices {
  my ($field_name, $spec) = @_;
  my $field = Bugzilla::Field->new({name => $field_name}) or return;

  my %declared = map { $_->{value} => 1 } @{$spec->{values}};
  my @extra
    = grep { $_ ne '' && $_ ne '---' && !$declared{$_} }
    map { $_->is_active ? $_->name : () }
    @{Bugzilla::Field::Choice->type($field_name)->match({})};

  fatal("$field_name has undeclared active value(s): " . join(', ', sort @extra)
      . '. Add them to expected-state.json or deactivate them.')
    if @extra;
  return;
}

########################################################################
# Statuses, resolutions, workflow
########################################################################

sub ensure_statuses {
  foreach my $spec (@{$STATE->{statuses}}) {
    my $status = Bugzilla::Status->new({name => $spec->{value}});

    if (!$status && $spec->{rename_from}) {
      my $old = Bugzilla::Status->new({name => $spec->{rename_from}});
      if ($old) {
        plan("rename status '$spec->{rename_from}' -> '$spec->{value}' (id "
            . $old->id . ', preserves history and workflow rows)');
        if (!$DRY) {
          $old->set('value', $spec->{value});
          $old->update;
          $status = $old;
        }
      }
    }

    if (!$status) {
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

  my %declared = map { $_->{value} => 1 } @{$STATE->{statuses}};
  my @extra = map { $_->is_active && !$declared{$_->name} ? $_->name : () }
    Bugzilla::Status->get_all;
  fatal('undeclared active status(es): ' . join(', ', sort @extra)
      . '. Add them to expected-state.json or deactivate them.')
    if @extra && !$DRY;
  return;
}

sub ensure_resolutions {
  ensure_choice('resolution', $_) foreach @{$STATE->{resolutions}};
  flush_caches() if !$DRY;

  return if $DRY;
  my %declared = map { $_->{value} => 1 } @{$STATE->{resolutions}};
  my @extra
    = grep { $_ ne '' && !$declared{$_} }
    map { $_->is_active ? $_->name : () }
    @{Bugzilla::Field::Choice->type('resolution')->match({})};
  fatal('undeclared active resolution(s): ' . join(', ', sort @extra)
      . '. Add them to expected-state.json or deactivate them.')
    if @extra;
  return;
}

sub rewrite_workflow {
  my %id_of;
  foreach my $spec (@{$STATE->{statuses}}) {
    my $status = Bugzilla::Status->new({name => $spec->{value}});
    if (!$status) {
      return note('workflow rewrite skipped in dry run (statuses not created yet)')
        if $DRY;
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
    my $err = $@ || "unknown error\n";
    eval { $dbh->bz_rollback_transaction(); 1 }
      or warn "WARNING: rollback also failed: $@";
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

  return if $DRY && !$product;

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

  my %declared = map { $_ => 1 } @{$spec->{versions}};
  my @extra = grep { !$declared{$_} && $have{$_}->is_active } keys %have;
  fatal("product '$spec->{name}' has undeclared active version(s): "
      . join(', ', sort @extra) . '. Add them to expected-state.json.')
    if @extra;
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

  my %declared = map { $_ => 1 } @{$spec->{milestones}};
  my @extra = grep { !$declared{$_} && $have{$_}->is_active } keys %have;
  fatal("product '$spec->{name}' has undeclared active milestone(s): "
      . join(', ', sort @extra) . '. Add them to expected-state.json.')
    if @extra;
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

  my %declared = map { $_->{name} => 1 } @{$spec->{components}};
  my @extra = grep { !$declared{$_} && $have{$_}->is_active } keys %have;
  fatal("product '$spec->{name}' has undeclared active component(s): "
      . join(', ', sort @extra) . '. Add them to expected-state.json.')
    if @extra;
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
    fatal("refusing to remove product '$name': it holds $count bug(s). "
        . 'Move or close them first.')
      if $count;

    plan("delete product '$name' (0 bugs)");
    next if $DRY;
    assert_lock_held("removing product '$name'");
    $product->remove_from_db;
  }
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

  # Order matters: cf_issue_type must exist before cf_category can be
  # controlled by it, and its values must exist before they can control others.
  ensure_field('cf_issue_type', $STATE->{fields}{cf_issue_type});
  ensure_choice('cf_issue_type', $_)
    foreach @{$STATE->{fields}{cf_issue_type}{values}};
  flush_caches() if !$DRY;

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

  if (!$DRY) {
    assert_no_undeclared_choices($_, $STATE->{fields}{$_})
      foreach sort keys %{$STATE->{fields}};
  }

  ensure_statuses();
  ensure_resolutions();
  ensure_product($_) foreach @{$STATE->{products}};

  # Last, because Bugzilla::Status::create() calls
  # add_missing_bug_status_transitions() and would otherwise leave rows behind.
  rewrite_workflow();

  ensure_params();
  decommission_products();

  flush_caches() if !$DRY;
  1;
};
my $err = $@;

release_lock();
die $err if !$ok;

if (!@PLAN) {
  print "\nNothing to do - the installation already matches expected-state.json.\n";
  exit 0;
}

printf("\n%s: %d action(s) %s\n",
  $DRY ? 'DRY RUN' : 'APPLIED', scalar(@PLAN), $DRY ? 'planned' : 'performed');
printf("%d action(s) executed against the database.\n", $APPLIED) if !$DRY;
print "\nNEXT: restart the Bugzilla web container, then run verify-projects.py.\n"
  if !$DRY;

exit 0;
