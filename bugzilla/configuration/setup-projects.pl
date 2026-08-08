#!/usr/bin/perl
#
# setup-projects.pl - converge the internal Bugzilla installation onto the
# configuration declared in expected-state.json.
#
# Bugzilla exposes Product.create and Component.create over REST, but bug
# statuses, the status_workflow matrix, custom fields and field-value control
# have no WebService at all. So this runs inside the Bugzilla web container and
# drives Bugzilla's own object APIs, with direct SQL used only where the 5.2
# object layer cannot reach (status_workflow rows, and bug_status.is_open, which
# is absent from Bugzilla::Field::Choice::UPDATE_COLUMNS).
#
# It is idempotent: re-running it against a converged installation reports zero
# mutations. It prints a plan and changes NOTHING unless --apply is given.
#
#   docker exec options-edge-bugzilla-web \
#     perl /var/www/html/local/setup-projects.pl \
#          --state /var/www/html/local/expected-state.json \
#          --admin-login <admin> --default-assignee <login>
#
# Add --apply to actually converge. See bugzilla/README.md for the full runbook.

BEGIN {
  my $root = $ENV{BUGZILLA_ROOT} || '/var/www/html';
  chdir($root) or die "FATAL: cannot chdir to Bugzilla root '$root': $!\n";
  unshift @INC, $root, "$root/lib", "$root/local/lib/perl5";
}

use 5.14.0;
use strict;
use warnings;

use Getopt::Long qw(GetOptions);
use JSON::PP     ();

use Bugzilla;
use Bugzilla::Constants;
use Bugzilla::Config qw(:admin);
use Bugzilla::Component;
use Bugzilla::Field;
use Bugzilla::Field::Choice;
use Bugzilla::Product;
use Bugzilla::Status;
use Bugzilla::User;

BEGIN { Bugzilla->extensions }

use constant LOCK_NAME => 'bugzilla-project-setup';

my %OPT = (
  state            => 'expected-state.json',
  'admin-login'    => undef,
  'default-assignee' => undef,
  apply            => 0,
  'remove-decommissioned' => 0,
  help             => 0,
);

GetOptions(\%OPT, 'state=s', 'admin-login=s', 'default-assignee=s', 'apply!',
  'remove-decommissioned!', 'help!')
  or die "FATAL: bad options. Try --help.\n";

if ($OPT{help}) {
  print <<'USAGE';
setup-projects.pl [options]

  --state FILE               expected-state.json (default: ./expected-state.json)
  --admin-login LOGIN        existing Bugzilla admin to act as (required)
  --default-assignee LOGIN   default assignee for newly created components
                             (default: --admin-login)
  --apply                    perform the changes (default is a dry run)
  --remove-decommissioned    also delete the products listed under
                             "decommission" (only if they hold zero bugs)
  --help                     this text
USAGE
  exit 0;
}

my @PLAN;      # human-readable actions, in order
my @MUTATIONS; # actions actually performed
my $DRY = !$OPT{apply};

sub plan {
  my ($msg) = @_;
  push @PLAN, $msg;
  push @MUTATIONS, $msg if !$DRY;
  printf("%s %s\n", $DRY ? '[plan ]' : '[apply]', $msg);
  return;
}

sub note { printf("[info ] %s\n", $_[0]); return; }

sub fatal { die "FATAL: $_[0]\n"; }

########################################################################
# Bootstrap
########################################################################

fatal("--admin-login is required") if !$OPT{'admin-login'};
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
fatal("'$OPT{'admin-login'}' is not in the 'admin' group")
  if !$admin->in_group('admin');
Bugzilla->set_user($admin);

my $default_assignee = Bugzilla::User->new({name => $OPT{'default-assignee'}})
  or fatal("default assignee '$OPT{'default-assignee'}' does not exist");
fatal("default assignee '$OPT{'default-assignee'}' is disabled")
  if !$default_assignee->is_enabled;

########################################################################
# Preflight
########################################################################

sub preflight {
  my $version = BUGZILLA_VERSION;
  fatal("expected Bugzilla $STATE->{bugzilla_version}, found $version")
    if $version ne $STATE->{bugzilla_version};

  my ($got_lock) = $dbh->selectrow_array('SELECT GET_LOCK(?, 0)', undef, LOCK_NAME);
  fatal("another setup run holds the '" . LOCK_NAME . "' lock") if !$got_lock;

  # Refuse to touch a status that real bugs are sitting on and that is not part
  # of the target model, rather than silently stranding those bugs.
  my %target_status = map { $_->{value} => 1 } @{$STATE->{statuses}};
  my %renamed_from
    = map { $_->{rename_from} => $_->{value} }
    grep { $_->{rename_from} } @{$STATE->{statuses}};

  my $rows = $dbh->selectall_arrayref(
    'SELECT bug_status, COUNT(*) FROM bugs GROUP BY bug_status', {Slice => {}});
  foreach my $row (@$rows) {
    my $status = $row->{bug_status};
    next if $target_status{$status} || $renamed_from{$status};
    fatal("bugs still use status '$status', which is not in the target model");
  }

  # An existing bug with no issue type could never be edited afterwards, because
  # the extension freezes untyped items except for their type field.
  my $type_field = Bugzilla::Field->new({name => 'cf_issue_type'});
  if ($type_field) {
    my ($untyped) = $dbh->selectrow_array(
      "SELECT COUNT(*) FROM bugs WHERE cf_issue_type IS NULL OR cf_issue_type IN ('', '---')"
    );
    note("$untyped existing bug(s) have no issue type; they will stay editable "
        . "only through cf_issue_type until it is set")
      if $untyped;
  }

  note("preflight OK (Bugzilla $version, acting as " . $admin->login . ")");
  return;
}

sub release_lock { $dbh->selectrow_array('SELECT RELEASE_LOCK(?)', undef, LOCK_NAME); }

# Bugzilla caches field/choice/status objects per request and in memcached.
# We are a long-lived process mutating exactly those objects, so drop the
# caches between phases.
sub flush_caches {
  my $cache = Bugzilla->request_cache;
  delete $cache->{fields};
  delete $cache->{active_custom_fields};
  delete $cache->{status_bug_state_open};
  foreach my $key (keys %$cache) {
    delete $cache->{$key}
      if $key
      =~ /^Bugzilla::(Field|Field::Choice|Status|Product|Component|Version|Milestone)/;
  }
  eval { Bugzilla->memcached->clear_all; 1 } or note("memcached flush skipped: $@");
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
        . ($spec->{is_mandatory} ? 1 : 0) . ")");
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

  if (my $vf = $spec->{value_field}) {
    my $controller = Bugzilla::Field->new({name => $vf})
      or fatal("value_field '$vf' for $name does not exist");
    my $current = $field->value_field ? $field->value_field->name : '';
    if ($current ne $vf) {
      plan("field $name: set value_field from '$current' to '$vf'");
      if (!$DRY) { $field->set('value_field_id', $controller->id); $changed = 1; }
    }
  }

  if (my $vis = $spec->{visibility_field}) {
    my $controller = Bugzilla::Field->new({name => $vis})
      or fatal("visibility_field '$vis' for $name does not exist");
    my $current = $field->visibility_field ? $field->visibility_field->name : '';
    if ($current ne $vis) {
      plan("field $name: set visibility_field from '$current' to '$vis'");
      if (!$DRY) { $field->set('visibility_field_id', $controller->id); $changed = 1; }
    }

    my @want = sort @{$spec->{visibility_values} || []};
    my @have = sort map { $_->name } @{$field->visibility_values || []};
    if (join('|', @want) ne join('|', @have)) {
      plan("field $name: set visibility_values to [" . join(', ', @want) . ']');
      if (!$DRY) {
        my @ids = map {
          Bugzilla::Field::Choice->type($vis)->new({name => $_})
            ? Bugzilla::Field::Choice->type($vis)->new({name => $_})->id
            : fatal("visibility value '$_' of field '$vis' does not exist")
        } @want;
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

  my $class  = Bugzilla::Field::Choice->type($field_name);
  my $choice = $class->new({name => $spec->{value}});

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
  if (defined $controller_id) {
    my $have = $choice->visibility_value ? $choice->visibility_value->id : 0;
    if ($have != $controller_id) {
      plan("$field_name value '$spec->{value}': controlled_by -> $spec->{controlled_by}");
      if (!$DRY) { $choice->set('visibility_value_id', $controller_id); $changed = 1; }
    }
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

    if (!$status && $spec->{rename_from}) {
      my $old = Bugzilla::Status->new({name => $spec->{rename_from}});
      if ($old) {
        plan("rename status '$spec->{rename_from}' -> '$spec->{value}' (id "
            . $old->id . ", preserves history and workflow rows)");
        if (!$DRY) {
          $old->set('value', $spec->{value});
          $old->update;
        }
        $status = $old;
      }
    }

    if (!$status) {
      plan("create status '$spec->{value}' (is_open=" . ($spec->{is_open} ? 1 : 0) . ')');
      next if $DRY;

      # is_open is settable only at create time: Bugzilla::Status inherits
      # Bugzilla::Field::Choice::UPDATE_COLUMNS, which does not include it.
      $status = Bugzilla::Status->create({
        value    => $spec->{value},
        sortkey  => $spec->{sortkey},
        isactive => 1,
        is_open  => $spec->{is_open} ? 1 : 0,
      });
      next;
    }

    next if $DRY && !$status;

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
  foreach my $spec (@{$STATE->{resolutions}}) {
    ensure_choice('resolution', $spec);
  }
  flush_caches() if !$DRY;
  return;
}

sub rewrite_workflow {
  my %id_of;
  foreach my $spec (@{$STATE->{statuses}}) {
    my $status = Bugzilla::Status->new({name => $spec->{value}});
    if (!$status) {
      return note("workflow rewrite skipped in dry run (statuses not created yet)")
        if $DRY;
      fatal("status '$spec->{value}' missing while rewriting the workflow");
    }
    $id_of{$spec->{value}} = $status->id;
  }

  my @want = map {
    [($_->{from} eq '' ? undef : $id_of{$_->{from}}),
      $id_of{$_->{to}}, ($_->{require_comment} ? 1 : 0)]
  } @{$STATE->{workflow}};

  foreach my $row (@want) {
    fatal("workflow references an unknown status") if !defined $row->[1];
  }

  my $have = $dbh->selectall_arrayref(
    'SELECT old_status, new_status, require_comment FROM status_workflow');

  my $key = sub {
    my ($r) = @_;
    return join('|', defined $r->[0] ? $r->[0] : 'NULL', $r->[1], $r->[2]);
  };
  my %have_set = map { $key->($_) => 1 } @$have;
  my %want_set = map { $key->($_) => 1 } @want;

  my $identical = (keys(%have_set) == keys(%want_set))
    && !grep { !$have_set{$_} } keys %want_set;

  if ($identical) {
    note('status_workflow already matches the declared matrix');
    return;
  }

  plan('rewrite status_workflow: ' . scalar(@$have) . ' row(s) -> '
      . scalar(@want) . ' row(s)');
  return if $DRY;

  $dbh->bz_start_transaction();
  $dbh->do('DELETE FROM status_workflow');
  my $sth = $dbh->prepare(
    'INSERT INTO status_workflow (old_status, new_status, require_comment) VALUES (?, ?, ?)'
  );
  $sth->execute(@$_) foreach @want;

  # Read back inside the transaction: a silently-wrong workflow is the single
  # most damaging failure this script can produce.
  my $check = $dbh->selectall_arrayref(
    'SELECT old_status, new_status, require_comment FROM status_workflow');
  my %check_set = map { $key->($_) => 1 } @$check;
  if (keys(%check_set) != keys(%want_set) || grep { !$check_set{$_} } keys %want_set) {
    $dbh->bz_rollback_transaction();
    fatal('status_workflow read-back did not match the declared matrix; rolled back');
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
    my $changed = 0;
    foreach my $attr (sort keys %want) {
      my $getter = $attr eq 'isactive' ? 'is_active'
        : $attr eq 'allows_unconfirmed' ? 'allows_unconfirmed'
        : $attr eq 'defaultmilestone'   ? 'default_milestone'
        :                                 $attr;
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

  foreach my $comp (@{$spec->{components}}) {
    my $existing
      = Bugzilla::Component->new({product => $product, name => $comp->{name}});
    if (!$existing) {
      plan("product '$spec->{name}': create component '$comp->{name}' "
          . "(default assignee " . $default_assignee->login . ')');
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
  return;
}

sub decommission_products {
  foreach my $name (@{$STATE->{decommission}{products} || []}) {
    my $product = Bugzilla::Product->new({name => $name}) or next;
    my $count = $product->bug_count;

    if ($count) {
      plan("product '$name' still holds $count bug(s): deactivating instead of deleting");
      if (!$DRY && $product->is_active) {
        $product->set('isactive', 0);
        $product->update;
      }
      next;
    }

    if (!$OPT{'remove-decommissioned'}) {
      note("product '$name' is empty and ready for removal "
          . '(re-run with --remove-decommissioned after verification)');
      next;
    }

    plan("delete product '$name' (0 bugs)");
    next if $DRY;
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
  write_params() if $changed && !$DRY;
  return;
}

########################################################################
# Main
########################################################################

preflight();

my $ok = eval {
  note($DRY
    ? 'DRY RUN - nothing will be changed. Re-run with --apply to converge.'
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

printf("\n%s: %d action(s) %s\n",
  $DRY ? 'DRY RUN' : 'APPLIED',
  scalar(@PLAN),
  $DRY ? 'planned' : 'performed');

print "\nNothing to do - the installation already matches expected-state.json.\n"
  if !@PLAN;

print "\nNEXT: restart the Bugzilla web container, then run verify-projects.py.\n"
  if @PLAN && !$DRY;

exit(0);
