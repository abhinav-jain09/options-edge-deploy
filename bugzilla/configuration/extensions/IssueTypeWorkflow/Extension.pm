package Bugzilla::Extension::IssueTypeWorkflow;

# Keeps the two issue-type lifecycles apart inside Bugzilla's single, global
# status workflow.
#
# Bugzilla 5.2 has exactly one bug_status table and one status_workflow matrix
# for the whole installation - there is no per-product and no per-issue-type
# workflow. Both lifecycles therefore live in that one matrix, which means core
# Bugzilla would happily let a bug be moved to REQ_APPROVED, or a requirement be
# resolved FIXED. This extension makes those moves impossible, on every path
# (UI, bulk edit, XML-RPC/JSON-RPC and REST all funnel through Bugzilla::Bug).
#
# The issue type is a per-bug field: the four products are Internal/External per
# project, and BOTH kinds hold bugs AND requirements, so the product cannot say
# which lifecycle an item is on.
#
# cf_issue_type is OPTIONAL at the database level and REQUIRED on creation by
# this extension. That split is deliberate. is_mandatory would apply to every
# future EDIT as well, and the installation already holds 178 items that have no
# type - they would all become uneditable. An item with no type is treated as
# LEGACY_TYPE below: those items predate the model and are all BUG-valid.
#
# See bugzilla/configuration/expected-state.json for the declarative model this
# mirrors, and docs/bugzilla-project-lifecycle.md for the design.

use 5.14.0;
use strict;
use warnings;

use parent qw(Bugzilla::Extension);

use Scalar::Util qw(blessed);

use Bugzilla::Constants;
use Bugzilla::Error;
use Bugzilla::Field;
use Bugzilla::Field::Choice;
use Bugzilla::Status;

our $VERSION = '3.0.0';

# Checked against expected-state.json's spec_version by setup-projects.pl, along
# with every policy constant below, so the two halves cannot drift apart. They
# are constants rather than runtime config reads on purpose: an unreadable or
# half-written config file must never be able to silently turn enforcement off.
use constant SPEC_VERSION => '3.0.0';

use constant TYPE_FIELD        => 'cf_issue_type';
use constant CATEGORY_FIELD    => 'cf_category';
use constant ENVIRONMENT_FIELD => 'cf_environment';

# What an item with NO type is. The 178 pre-existing items are internal
# engineering bugs on CONFIRMED/IN_PROGRESS/RESOLVED+FIXED - every one of which
# is BUG-valid - so treating them as BUG needs no migration and leaves them
# editable.
use constant LEGACY_TYPE => 'BUG';

# The unset sentinel Bugzilla creates for every custom select field.
use constant UNSET => '---';

use constant GUARDED_FIELDS =>
  {TYPE_FIELD, 1, CATEGORY_FIELD, 1, 'bug_status', 1, 'resolution', 1};

use constant INITIAL_STATUS => {BUG => 'UNCONFIRMED', REQUIREMENT => 'REQ_DRAFT',};

# The BUG branch keeps Bugzilla's stock status names. Renaming them would be an
# UPDATE against the live bugs sitting on them (Bugzilla/Field/Choice.pm:158),
# which the no-touch constraint forbids.
use constant ALLOWED_STATUSES => {
  BUG => {map { $_ => 1 } qw(UNCONFIRMED CONFIRMED IN_PROGRESS RESOLVED VERIFIED)},
  REQUIREMENT => {
    map { $_ => 1 }
      qw(REQ_DRAFT REQ_REVIEW REQ_APPROVED REQ_IN_PROGRESS RESOLVED VERIFIED)
  },
};

use constant STATUS_IS_OPEN => {
  UNCONFIRMED     => 1,
  CONFIRMED       => 1,
  IN_PROGRESS     => 1,
  REQ_DRAFT       => 1,
  REQ_REVIEW      => 1,
  REQ_APPROVED    => 1,
  REQ_IN_PROGRESS => 1,
  RESOLVED        => 0,
  VERIFIED        => 0,
};

use constant WORKFLOW => [
  ['',                'UNCONFIRMED',     0],
  ['',                'REQ_DRAFT',       0],
  ['UNCONFIRMED',     'CONFIRMED',       0],
  ['UNCONFIRMED',     'IN_PROGRESS',     0],
  ['UNCONFIRMED',     'RESOLVED',        1],
  ['CONFIRMED',       'IN_PROGRESS',     0],
  ['CONFIRMED',       'RESOLVED',        1],
  ['IN_PROGRESS',     'CONFIRMED',       1],
  ['IN_PROGRESS',     'RESOLVED',        1],
  ['REQ_DRAFT',       'REQ_REVIEW',      0],
  ['REQ_DRAFT',       'RESOLVED',        1],
  ['REQ_REVIEW',      'REQ_DRAFT',       1],
  ['REQ_REVIEW',      'REQ_APPROVED',    0],
  ['REQ_REVIEW',      'RESOLVED',        1],
  ['REQ_APPROVED',    'REQ_REVIEW',      1],
  ['REQ_APPROVED',    'REQ_IN_PROGRESS', 0],
  ['REQ_APPROVED',    'RESOLVED',        1],
  ['REQ_IN_PROGRESS', 'REQ_APPROVED',    1],
  ['REQ_IN_PROGRESS', 'RESOLVED',        1],
  ['RESOLVED',        'CONFIRMED',       1],
  ['RESOLVED',        'REQ_REVIEW',      1],
  ['RESOLVED',        'VERIFIED',        0],
  ['VERIFIED',        'CONFIRMED',       1],
  ['VERIFIED',        'REQ_REVIEW',      1],
  ['VERIFIED',        'RESOLVED',        1],
];

use constant ALLOWED_RESOLUTIONS => {
  BUG => {map { $_ => 1 } qw(FIXED INVALID WONTFIX DUPLICATE WORKSFORME)},
  REQUIREMENT => {map { $_ => 1 } qw(IMPLEMENTED REJECTED DEFERRED DUPLICATE)},
};

# Compiled in as well as present in the database, and both must agree - so that
# adding a value through editvalues.cgi cannot widen the vocabulary.
use constant ALLOWED_CATEGORIES => {
  BUG => {
    map { $_ => 1 }
      qw(BUG_INFRA BUG_CODE BUG_CONFIG BUG_DATA BUG_INTEGRATION BUG_CI_CD
      BUG_SECURITY BUG_PERFORMANCE BUG_RELIABILITY BUG_TEST BUG_DOCUMENTATION)
  },
  REQUIREMENT => {
    map { $_ => 1 }
      qw(REQ_COSMETIC REQ_LOGIC REQ_WORKFLOW REQ_DATA REQ_INTEGRATION
      REQ_ACCESS_CONTROL REQ_SECURITY REQ_COMPLIANCE REQ_PERFORMANCE
      REQ_OPERABILITY REQ_REPORTING REQ_DOCUMENTATION)
  },
};

# Stable WebService codes, so a caller (and verify-projects.py) can tell "the
# policy refused this" apart from an auth failure, a 404 or a 500.
use constant ERROR_CODES => {
  issue_type_required                     => 100001,
  issue_type_unknown                      => 100002,
  issue_category_mismatch                 => 100004,
  issue_type_initial_status_unavailable   => 100005,
  issue_type_initial_status_needs_comment => 100006,
  issue_type_workflow_misconfigured       => 100007,
  issue_type_value_rename_forbidden       => 100009,
  issue_type_value_delete_forbidden       => 100010,
};

###############################
# Enforcement state           #
###############################

# Three states, none switchable from the admin UI.
#
#   off    - cf_category does not exist AND no REQ_* status does either: this
#            installation has never been provisioned, so checksetup.pl, the
#            first extension mount and the first apply all work normally.
#   on     - the whole declared model is present and matches.
#   broken - anything else. EVERY guarded change is refused until
#            setup-projects.pl has been re-run.
#
# There is deliberately no "enforcement enabled" parameter: any such switch is
# reachable by anyone with tweakparams. Deleting cf_category is not a way out
# either: the REQ_* statuses remain, so the state is 'broken', not 'off'.
#
# Renaming a choice value would rewrite bug rows directly - Field::Choice::update
# runs UPDATE bugs SET <field> = ? without going through Bugzilla::Bug - so that
# is guarded separately, in object_before_set, which fires before any of it.
#
# Cached for the current request only, so no answer outlives the state it
# described and a mod_perl worker cannot get stuck fail-open.
sub _enforcement_state {
  my $cache = Bugzilla->request_cache;
  return $cache->{itw_state} if $cache->{itw_state};

  my $state;
  if (!Bugzilla::Field->new({name => CATEGORY_FIELD})) {

    # Bootstrap, or sabotage? The difference is whether this installation has
    # EVER been provisioned, and the database can answer that: provisioning
    # creates the REQ_* statuses, which cannot be removed while any item sits
    # on them.
    #
    # An env var is not usable here (it would brick every web request between
    # mounting the extension and the first apply), and "not a CGI request" is
    # not usable either (it would leave email-in and cron unenforced the moment
    # someone deleted the field).
    $state = _ever_provisioned() ? 'broken' : 'off';
  }
  else {
    $state = model_is_complete() ? 'on' : 'broken';
  }

  return $cache->{itw_state} = $state;
}

# Everything that can change what the policy PERMITS - and deliberately not
# more. A re-pointed category, a flipped is_open, a deleted creation edge or an
# editworkflow.cgi shortcut all change what is allowed, so all of them are
# checked. Cosmetics (descriptions, sort keys, buglist flags) cannot, so they
# are left to the provisioner's zero-change dry run rather than paid for on
# every request.
#
# Undeclared statuses, resolutions and products are not checked either, because
# they cannot widen anything: the workflow matrix is pinned exactly, so an extra
# status is unreachable; an extra resolution is refused by ALLOWED_RESOLUTIONS;
# and an item in an unmapped product has no type, so its guarded fields are
# frozen. The provisioner still refuses to run while any of them exist.
#
# Public because setup-projects.pl calls it to prove, inside a transaction it
# then rolls back, that damage really does trip 'broken'.
# True once provisioning has happened, judged by something an administrator
# cannot quietly remove: the requirement statuses. Deleting cf_category alone
# therefore lands in 'broken' rather than back in 'bootstrap'.
sub _ever_provisioned {
  foreach my $name (keys %{ALLOWED_STATUSES->{REQUIREMENT}}) {
    next if ALLOWED_STATUSES->{BUG}{$name};    # skip the shared closed tail
    return 1 if Bugzilla::Status->new({name => $name});
  }
  return 0;
}

sub model_is_complete {
  my $field = Bugzilla::Field->new({name => CATEGORY_FIELD}) or return 0;
  return 0 if !$field->custom;
  return 0 if $field->type != FIELD_TYPE_SINGLE_SELECT;
  return 0 if $field->obsolete;
  return 0 if !$field->enter_bug;

  # Optional by design: this is what leaves the pre-existing bugs alone.
  return 0 if $field->is_mandatory;

  # Flat and uncontrolled: a controller on the category would make an
  # editvalues.cgi edit the policy.
  return 0 if $field->value_field;
  return 0 if $field->visibility_field;

  # cf_environment carries no policy, but it is part of the declared model. It is
  # uncontrolled now that Internal/External no longer means bug-vs-requirement.
  my $env = Bugzilla::Field->new({name => ENVIRONMENT_FIELD}) or return 0;
  return 0 if !$env->custom || $env->obsolete;
  return 0 if $env->type != FIELD_TYPE_SINGLE_SELECT;
  return 0 if $env->is_mandatory;
  return 0 if $env->visibility_field;

  # The type field itself, and exactly the two declared values.
  my $type_field = Bugzilla::Field->new({name => TYPE_FIELD}) or return 0;
  return 0 if !$type_field->custom || $type_field->obsolete;
  return 0 if $type_field->type != FIELD_TYPE_SINGLE_SELECT;
  return 0 if !$type_field->enter_bug;

  # Optional by design: is_mandatory would lock every legacy item out of editing.
  return 0 if $type_field->is_mandatory;

  my %live_type = map { $_->name => 1 }
    grep { $_->name ne UNSET && $_->is_active }
    @{Bugzilla::Field::Choice->type(TYPE_FIELD)->match({})};
  return 0 if keys %live_type != keys %{(ALLOWED_STATUSES)};
  foreach my $type (keys %{(ALLOWED_STATUSES)}) {
    return 0 if !$live_type{$type};
  }

  my %live;
  foreach my $choice (@{Bugzilla::Field::Choice->type(CATEGORY_FIELD)->match({})}) {
    next if $choice->name eq UNSET;
    return 0 if !$choice->is_active;
    $live{$choice->name} = 1;
  }
  my %want;
  foreach my $type (keys %{(ALLOWED_CATEGORIES)}) {
    $want{$_} = 1 foreach keys %{ALLOWED_CATEGORIES->{$type}};
  }
  return 0 if keys %live != keys %want;
  foreach my $category (keys %want) {
    return 0 if !$live{$category};
  }

  my %live_resolution
    = map { $_->name => 1 }
    grep { $_->is_active }
    @{Bugzilla::Field::Choice->type('resolution')->match({})};
  foreach my $type (keys %{(ALLOWED_RESOLUTIONS)}) {
    foreach my $resolution (keys %{ALLOWED_RESOLUTIONS->{$type}}) {
      return 0 if !$live_resolution{$resolution};
    }
  }

  my %status_id;
  foreach my $name (keys %{(STATUS_IS_OPEN)}) {
    my $status = Bugzilla::Status->new({name => $name}) or return 0;
    return 0 if !$status->is_active;
    return 0 if (($status->is_open ? 1 : 0) != STATUS_IS_OPEN->{$name});
    $status_id{$name} = $status->id;
  }

  # The matrix, exactly, as a multiset: status_workflow's UNIQUE index does not
  # constrain rows whose old_status is NULL.
  my %want_edge;
  foreach my $edge (@{(WORKFLOW)}) {
    my ($from, $to, $comment) = @$edge;
    my $from_id = $from eq '' ? 'NULL' : $status_id{$from};
    return 0 if !defined $from_id;
    $want_edge{"$from_id|$status_id{$to}|$comment"}++;
  }
  my $rows = Bugzilla->dbh->selectall_arrayref(
    'SELECT old_status, new_status, require_comment FROM status_workflow');
  my %got_edge;
  foreach my $row (@$rows) {
    $got_edge{(defined $row->[0] ? $row->[0] : 'NULL') . "|$row->[1]|$row->[2]"}++;
  }
  return 0 if keys %got_edge != keys %want_edge;
  foreach my $key (keys %want_edge) {
    return 0 if ($got_edge{$key} // 0) != $want_edge{$key};
  }

  return 1;
}

###############################
# Type derivation             #
###############################

sub _is_known_type {
  my ($type) = @_;
  return 0 if !defined $type || $type eq '' || $type eq UNSET;
  return ALLOWED_STATUSES->{$type} ? 1 : 0;
}

# An item with no type is LEGACY: it predates the model, and treating it as
# LEGACY_TYPE is what keeps the 178 pre-existing items working untouched.
sub _type_of_bug {
  my ($bug) = @_;
  my $accessor = TYPE_FIELD;
  my $type = $bug->can($accessor) ? $bug->$accessor : $bug->{+TYPE_FIELD};
  return LEGACY_TYPE if !_is_known_type($type);
  return $type;
}

# A category is valid for a type only if the compiled-in vocabulary and the live
# configuration agree. An empty category is ALWAYS valid: that is what leaves
# the pre-existing bugs - which have none - freely editable.
sub _category_ok {
  my ($type, $category) = @_;
  return 1 if !defined $category || $category eq '' || $category eq UNSET;
  return 0 if !defined $type;
  return 0 if !ALLOWED_CATEGORIES->{$type}{$category};
  my $choice
    = Bugzilla::Field::Choice->type(CATEGORY_FIELD)->new({name => $category});
  return $choice ? 1 : 0;
}

###########
#  Hooks  #
###########

# Runs for every guarded field that actually changes, on every update path, via
# Bugzilla::Object::set -> Bugzilla::Bug::_set_global_validator ->
# check_can_change_field.
#
# We deny by pushing into priv_results rather than throwing:
#
#  1. Bug.pm:4558 takes the first entry > 0 and returns 0 immediately, WITHOUT
#     consulting the user's real privileges - a hard deny for everybody,
#     administrators included.
#  2. _refine_available_statuses() calls check_can_change_field() just to build
#     the status dropdown. Throwing would blow up every bug page; denying
#     filters the dropdown to the item's own lifecycle for free.
#
# This hook must never throw.
sub bug_check_can_change_field {
  my ($self, $args) = @_;
  my ($bug, $field, $new_value, $priv_results)
    = @$args{qw(bug field new_value priv_results)};

  return if !defined $field || !GUARDED_FIELDS->{$field};

  my $state = _enforcement_state();
  return if $state eq 'off';
  return _deny($priv_results) if $state eq 'broken';

  return if !blessed($bug) || !$bug->isa('Bugzilla::Bug') || !$bug->id;

  my $type = _type_of_bug($bug);

  # The type is immutable once set - changing it would strand the item on a
  # status and category belonging to the other lifecycle. A LEGACY item (no
  # stored type) may be typed once, and only to a type its CURRENT status,
  # resolution and category are all already valid for, because those fields are
  # unchanged and so never reach their own checks.
  if ($field eq TYPE_FIELD) {
    my $accessor = TYPE_FIELD;
    my $stored = $bug->can($accessor) ? $bug->$accessor : $bug->{+TYPE_FIELD};
    return _deny($priv_results) if _is_known_type($stored);
    return _deny($priv_results) if !_is_known_type($new_value);
    return _deny($priv_results)
      if !ALLOWED_STATUSES->{$new_value}{$bug->status->name};
    my $resolution = $bug->resolution;
    return _deny($priv_results)
      if defined $resolution
      && $resolution ne ''
      && !ALLOWED_RESOLUTIONS->{$new_value}{$resolution};
    my $cat_accessor = CATEGORY_FIELD;
    my $category
      = $bug->can($cat_accessor) ? $bug->$cat_accessor : $bug->{+CATEGORY_FIELD};
    return _deny($priv_results) if !_category_ok($new_value, $category);
    return;
  }

  if ($field eq 'bug_status') {
    return _deny($priv_results) if !ALLOWED_STATUSES->{$type}{$new_value};
    return;
  }

  if ($field eq 'resolution') {

    # Clearing the resolution (reopening) is always allowed; core keeps the
    # open-status/empty-resolution invariant itself.
    return if !defined $new_value || $new_value eq '';
    return _deny($priv_results) if !ALLOWED_RESOLUTIONS->{$type}{$new_value};
    return;
  }

  if ($field eq CATEGORY_FIELD) {
    return _deny($priv_results) if !_category_ok($type, $new_value);
    return;
  }

  return;
}

# Runs at the end of Bugzilla::Bug::run_create_validators: after every field has
# been validated but before any row is written. bug_status is a Bugzilla::Status
# OBJECT (Bug.pm:1540), custom selects are strings, and the product has already
# been resolved into $params->{product_id}.
sub bug_end_of_create_validators {
  my ($self, $args) = @_;
  my $params = $args->{params};

  my $state = _enforcement_state();
  return if $state eq 'off';
  ThrowUserError('issue_type_workflow_misconfigured') if $state eq 'broken';

  # Required on creation, so every NEW item is typed, while the pre-existing
  # untyped ones stay editable (is_mandatory would have blocked those too).
  my $type = $params->{+TYPE_FIELD};
  ThrowUserError('issue_type_required')
    if !defined $type || $type eq '' || $type eq UNSET;
  ThrowUserError('issue_type_unknown', {issue_type => $type})
    if !_is_known_type($type);

  my $category = $params->{+CATEGORY_FIELD};
  ThrowUserError('issue_category_mismatch',
    {issue_type => $type, category => $category})
    if !_category_ok($type, $category);

  # Canonicalise the entry point. Not just convenience: Bug.pm:1526-1536 forces
  # UNCONFIRMED for any reporter without editbugs/canconfirm, which would drop
  # every externally filed requirement onto the BUG lifecycle.
  my $wanted = INITIAL_STATUS->{$type};
  my ($initial)
    = grep { $_->name eq $wanted } @{Bugzilla::Status->can_change_to};
  ThrowUserError('issue_type_initial_status_unavailable',
    {issue_type => $type, status => $wanted})
    if !$initial;

  # Core validated the comment requirement of the status the caller asked for,
  # not of the one we are substituting.
  my $comment = $params->{comment};
  my $text
    = ref($comment) eq 'HASH'  ? $comment->{body}
    : ref($comment) eq 'ARRAY' ? join('', grep { !ref } @$comment)
    : ref($comment)            ? undef
    :                            $comment;
  ThrowUserError('issue_type_initial_status_needs_comment',
    {issue_type => $type, status => $wanted})
    if $initial->comment_required_on_change_from(undef)
    && !(defined $text && $text =~ /\S/);

  $params->{bug_status} = $initial;

  # _check_bug_status derived everconfirmed from the status it saw
  # (Bug.pm:1584), before we replaced it.
  $params->{everconfirmed} = $initial->name eq 'UNCONFIRMED' ? 0 : 1;

  # Both entry points are open statuses; an item may never be born resolved.
  $params->{resolution} = '';

  return;
}

# Bugzilla::Field::Choice::update implements a value RENAME as a direct
# UPDATE bugs SET <field> = ? (Field/Choice.pm:158) and a status/resolution
# rename therefore rewrites live bug rows without Bugzilla::Bug - and so without
# any of the hooks above - ever being involved. An administrator with editvalues
# could rename BUG_CODE to REQ_LOGIC, or swap two statuses through temporary
# names, and land in a state that looks complete again afterwards.
#
# object_before_set (Object.pm:445) fires before Bugzilla::Object::set does any
# work at all, which is early enough to refuse it.
sub object_before_set {
  my ($self, $args) = @_;
  my ($object, $field) = @$args{qw(object field)};

  return if _enforcement_state() eq 'off';
  return if !blessed($object);

  # Renaming a value the policy names.
  if ($object->isa('Bugzilla::Field::Choice') && $field eq 'value') {
    my $of = eval { $object->field->name } // '';
    my $current = eval { $object->name } // '';
    return if !GUARDED_FIELDS->{$of};

    my $guarded
      = $of eq 'bug_status' ? STATUS_IS_OPEN->{$current}
      : $of eq 'resolution'
      ? (ALLOWED_RESOLUTIONS->{BUG}{$current}
        || ALLOWED_RESOLUTIONS->{REQUIREMENT}{$current})
      : (ALLOWED_CATEGORIES->{BUG}{$current}
        || ALLOWED_CATEGORIES->{REQUIREMENT}{$current});
    ThrowUserError('issue_type_value_rename_forbidden',
      {field => $of, value => $current})
      if defined $guarded;
  }


  return;
}

# Same reasoning: deleting one of these would strip the lifecycle out from under
# live items.
sub object_before_delete {
  my ($self, $args) = @_;
  my $object = $args->{object};

  return if _enforcement_state() eq 'off';
  return if !blessed($object);

  if ($object->isa('Bugzilla::Field::Choice')) {
    my $of      = eval { $object->field->name } // '';
    my $current = eval { $object->name } // '';
    return if !GUARDED_FIELDS->{$of};
    my $guarded
      = $of eq 'bug_status' ? STATUS_IS_OPEN->{$current}
      : $of eq 'resolution'
      ? (ALLOWED_RESOLUTIONS->{BUG}{$current}
        || ALLOWED_RESOLUTIONS->{REQUIREMENT}{$current})
      : (ALLOWED_CATEGORIES->{BUG}{$current}
        || ALLOWED_CATEGORIES->{REQUIREMENT}{$current});
    ThrowUserError('issue_type_value_delete_forbidden',
      {field => $of, value => $current})
      if defined $guarded;
  }


  return;
}

sub webservice_error_codes {
  my ($self, $args) = @_;
  my $error_map = $args->{error_map};
  $error_map->{$_} = ERROR_CODES->{$_} foreach keys %{(ERROR_CODES)};
  return;
}

##############
#  Internals #
##############

sub _deny {
  my ($priv_results) = @_;
  push @$priv_results, PRIVILEGES_REQUIRED_EMPOWERED;
  return;
}

__PACKAGE__->NAME;
