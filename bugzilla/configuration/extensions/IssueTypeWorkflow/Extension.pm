package Bugzilla::Extension::IssueTypeWorkflow;

# Keeps the two issue-type lifecycles apart inside Bugzilla's single, global
# status workflow.
#
# Bugzilla 5.2 has exactly one bug_status table and one status_workflow matrix
# for the whole installation - there is no per-product and no per-issue-type
# workflow. Both the BUG lifecycle and the REQUIREMENT lifecycle therefore live
# in that one matrix, which means core Bugzilla would happily let a BUG be moved
# to REQ_APPROVED, or a REQUIREMENT be resolved FIXED. This extension is what
# makes those moves impossible, on every path (UI, bulk edit, XML-RPC/JSON-RPC
# and REST all funnel through Bugzilla::Bug).
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

our $VERSION = '1.0.0';

# Checked against expected-state.json's spec_version by setup-projects.pl, so
# the two halves of the policy cannot drift apart unnoticed. The policy maps
# below are likewise cross-checked against the JSON on every provisioning run.
#
# They are duplicated as constants rather than read from the JSON at runtime on
# purpose: an unreadable, truncated or half-written config file must never be
# able to silently turn enforcement off.
use constant SPEC_VERSION => '1.0.0';

use constant TYPE_FIELD     => 'cf_issue_type';
use constant CATEGORY_FIELD => 'cf_category';

# The unset sentinel Bugzilla creates for every custom select field.
use constant UNSET => '---';

use constant GUARDED_FIELDS =>
  {TYPE_FIELD, 1, CATEGORY_FIELD, 1, 'bug_status', 1, 'resolution', 1};

use constant INITIAL_STATUS => {BUG => 'UNCONFIRMED', REQUIREMENT => 'REQ_DRAFT',};

use constant ALLOWED_STATUSES => {
  BUG => {
    map { $_ => 1 }
      qw(UNCONFIRMED BUG_CONFIRMED BUG_IN_PROGRESS RESOLVED VERIFIED)
  },
  REQUIREMENT => {
    map { $_ => 1 }
      qw(REQ_DRAFT REQ_REVIEW REQ_APPROVED REQ_IN_PROGRESS RESOLVED VERIFIED)
  },
};

use constant ALLOWED_RESOLUTIONS => {
  BUG => {map { $_ => 1 } qw(FIXED INVALID WONTFIX DUPLICATE WORKSFORME)},
  REQUIREMENT => {map { $_ => 1 } qw(IMPLEMENTED REJECTED DEFERRED DUPLICATE)},
};

# The category vocabulary is compiled in as well as being expressed in the
# database (each cf_category value's visibility_value_id). Both must agree.
#
# Reading only the database would make an administrator's editvalues.cgi edit
# the policy: adding a category would silently widen the vocabulary, and
# re-pointing one would silently change what an already-stored item means.
# Reading only the constants would miss a value that has been re-pointed. So a
# category is accepted only if the constant AND the live controller agree.
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

# Set by setup-projects.pl once the whole model is in place. See lib/Config.pm.
use constant ENFORCED_PARAM => 'issue_type_workflow_enforced';

# Stable WebService codes for the errors this extension raises, so a caller (and
# verify-projects.py) can tell "the policy refused this" apart from an auth
# failure, a 404 or a 500. Bugzilla reserves codes below 100000 for itself.
# Codes not in REST_STATUS_CODE_MAP fall through to its _default, HTTP 400.
use constant ERROR_CODES => {
  issue_type_required                    => 100001,
  issue_type_unknown                     => 100002,
  issue_category_required                => 100003,
  issue_category_mismatch                => 100004,
  issue_type_initial_status_unavailable  => 100005,
  issue_type_initial_status_needs_comment => 100006,
  issue_type_workflow_misconfigured      => 100007,
};

#############################
# Provisioning-state helpers #
#############################

# Enforcement has three states, not two.
#
#   off    - issue_type_workflow_enforced is unset. The installation has not
#            been provisioned yet (this is also what lets the very first
#            checksetup.pl run load the extension before the fields exist).
#            Nothing is enforced.
#   on     - the parameter is set and the whole model is present. Normal policy.
#   broken - the parameter is set but something the model needs has been
#            removed or renamed. Every guarded change is refused until
#            setup-projects.pl has been re-run.
#
# The 'broken' state is the whole point of having a persistent marker: without
# it, deleting one status through editvalues.cgi would silently turn
# enforcement off instead of stopping the installation.
#
# Only 'on' is cached. A negative answer must never be remembered, or a
# long-lived mod_perl worker that once saw an unprovisioned database would stay
# permanently fail-open.
sub _enforcement_state {
  my $cache = Bugzilla->request_cache;
  return 'on' if $cache->{itw_enforcing};

  return 'off' if !Bugzilla->params->{+ENFORCED_PARAM};
  return 'broken' if !_model_is_complete();

  $cache->{itw_enforcing} = 1;
  return 'on';
}

sub _model_is_complete {
  return 0 if !Bugzilla::Field->new({name => TYPE_FIELD});
  return 0 if !Bugzilla::Field->new({name => CATEGORY_FIELD});

  foreach my $type (keys %{(INITIAL_STATUS)}) {
    return 0
      if !Bugzilla::Field::Choice->type(TYPE_FIELD)->new({name => $type});
  }

  my %needed;
  foreach my $type (keys %{(ALLOWED_STATUSES)}) {
    $needed{$_} = 1 foreach keys %{ALLOWED_STATUSES->{$type}};
  }
  foreach my $status (keys %needed) {
    return 0 if !Bugzilla::Status->new({name => $status});
  }

  return 1;
}

sub _is_known_type {
  my ($type) = @_;
  return 0 if !defined $type || $type eq '' || $type eq UNSET;
  return exists INITIAL_STATUS->{$type} ? 1 : 0;
}

sub _type_of {
  my ($bug) = @_;
  my $accessor = TYPE_FIELD;
  return $bug->can($accessor) ? $bug->$accessor : $bug->{+TYPE_FIELD};
}

sub _category_of {
  my ($bug) = @_;
  my $accessor = CATEGORY_FIELD;
  return $bug->can($accessor) ? $bug->$accessor : $bug->{+CATEGORY_FIELD};
}

# A category is valid for a type only if the compiled-in vocabulary and the live
# configuration BOTH say so (see ALLOWED_CATEGORIES).
#
# Note Bugzilla 5.2 allows exactly ONE controlling value per field value - the
# value tables carry a single visibility_value_id column - which is why the two
# category vocabularies are prefixed (BUG_*/REQ_*) instead of sharing names.
#
# Empty and unknown values are rejected here as well as by the field's
# is_mandatory flag: an administrator clearing that flag must not silently open
# an "uncategorised item" bypass.
sub _category_matches_type {
  my ($type, $category) = @_;

  return 0 if !defined $category || $category eq '' || $category eq UNSET;

  # The declared vocabulary. An administrator cannot widen it from the admin UI.
  return 0 if !ALLOWED_CATEGORIES->{$type}{$category};

  my $choice
    = Bugzilla::Field::Choice->type(CATEGORY_FIELD)->new({name => $category});
  return 0 if !$choice;

  my $controller = $choice->visibility_value;

  # A category value with no controlling issue type is a provisioning fault.
  # Refuse it rather than let an unclassifiable value through.
  return 0 if !$controller;

  return $controller->name eq $type ? 1 : 0;
}

sub _resolution_ok {
  my ($type, $resolution) = @_;
  return 1 if !defined $resolution || $resolution eq '';
  return ALLOWED_RESOLUTIONS->{$type}{$resolution} ? 1 : 0;
}

###########
#  Hooks  #
###########

# Runs for every field that actually changes, on every update path, via
# Bugzilla::Object::set -> Bugzilla::Bug::_set_global_validator ->
# check_can_change_field (Bugzilla/Bug.pm).
#
# We deny by pushing into priv_results rather than by throwing. Two reasons:
#
#  1. Bug.pm:4558 takes the first priv_results entry > 0 and returns 0
#     immediately, WITHOUT consulting the user's real privileges - so this is a
#     hard deny for everybody, administrators included.
#  2. Bugzilla::Bug::_refine_available_statuses() calls check_can_change_field()
#     just to build the status dropdown. Throwing here would blow up rendering
#     of every bug page; denying instead makes the dropdown show only the
#     statuses of the item's own lifecycle, for free.
#
# A genuine attempt (rather than list-building) turns the 0 into a hard
# 'illegal_change' error inside _set_global_validator, which is what REST and
# the UI report. This hook must never throw.
sub bug_check_can_change_field {
  my ($self, $args) = @_;
  my ($bug, $field, $new_value, $priv_results)
    = @$args{qw(bug field new_value priv_results)};

  return if !defined $field || !GUARDED_FIELDS->{$field};

  my $state = _enforcement_state();
  return if $state eq 'off';

  # Something the model needs has gone missing. Refuse every guarded change
  # rather than quietly reverting to no policy at all.
  return _deny($priv_results) if $state eq 'broken';

  # New bugs are handled by bug_end_of_create_validators; there is no stored
  # type to judge against until the bug exists.
  return if !blessed($bug) || !$bug->isa('Bugzilla::Bug') || !$bug->id;

  my $type = _type_of($bug);

  # An item with no usable type can only be repaired by setting one, and only
  # to a type its CURRENT status, resolution and category are all valid for -
  # otherwise the repair itself would create the cross-lifecycle item this
  # extension exists to prevent (the other fields are unchanged, so their own
  # hook invocations never happen).
  if (!_is_known_type($type)) {
    return _deny($priv_results) if $field ne TYPE_FIELD;
    return _deny($priv_results) if !_is_known_type($new_value);
    return _deny($priv_results)
      if !ALLOWED_STATUSES->{$new_value}{$bug->status->name};
    return _deny($priv_results)
      if !_resolution_ok($new_value, $bug->resolution);
    return _deny($priv_results)
      if !_category_matches_type($new_value, _category_of($bug));
    return;
  }

  # The issue type is immutable once set. Changing it would strand the item on
  # a status and category belonging to the other lifecycle.
  return _deny($priv_results) if $field eq TYPE_FIELD;

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
    return _deny($priv_results) if !_category_matches_type($type, $new_value);
    return;
  }

  return;
}

# Runs at the end of Bugzilla::Bug::run_create_validators, i.e. after every
# field has been validated but before any row is written. At this point
# bug_status is a Bugzilla::Status OBJECT (Bug.pm:1540) while custom select
# fields are plain strings (Bug.pm:_check_select_field returns $object->name).
sub bug_end_of_create_validators {
  my ($self, $args) = @_;
  my $params = $args->{params};

  my $state = _enforcement_state();
  return if $state eq 'off';
  ThrowUserError('issue_type_workflow_misconfigured') if $state eq 'broken';

  my $type = $params->{+TYPE_FIELD};
  ThrowUserError('issue_type_required')
    if !defined $type || $type eq '' || $type eq UNSET;
  ThrowUserError('issue_type_unknown', {issue_type => $type})
    if !_is_known_type($type);

  my $category = $params->{+CATEGORY_FIELD};
  ThrowUserError('issue_category_required')
    if !defined $category || $category eq '' || $category eq UNSET;
  ThrowUserError('issue_category_mismatch',
    {issue_type => $type, category => $category})
    if !_category_matches_type($type, $category);

  # Canonicalise the entry point. This is not just convenience: Bug.pm:1526-1536
  # forces UNCONFIRMED for any reporter without editbugs/canconfirm, which would
  # otherwise drop every externally filed REQUIREMENT onto the BUG lifecycle.
  my $wanted = INITIAL_STATUS->{$type};

  # We are replacing a status core already validated, so re-check the one thing
  # that validation guaranteed: that it is actually a legal status to create a
  # bug in. If an administrator removes that row from the workflow, filing must
  # fail loudly rather than write an unreachable status.
  my ($initial) = grep { $_->name eq $wanted } @{Bugzilla::Status->can_change_to};
  ThrowUserError('issue_type_initial_status_unavailable',
    {issue_type => $type, status => $wanted})
    if !$initial;

  # Core validated the comment requirement of the status the caller asked for,
  # not of the one we are substituting.
  my $needs_comment = $initial->comment_required_on_change_from(undef);
  my $comment       = $params->{comment};
  my $has_comment
    = ref($comment) ? 1 : (defined $comment && $comment =~ /\S/) ? 1 : 0;
  ThrowUserError('issue_type_initial_status_needs_comment',
    {issue_type => $type, status => $wanted})
    if $needs_comment && !$has_comment;

  $params->{bug_status} = $initial;

  # _check_bug_status derived everconfirmed from the status it saw (Bug.pm:1584),
  # before we replaced it. Recompute it or the row is written inconsistent.
  $params->{everconfirmed} = $initial->name eq 'UNCONFIRMED' ? 0 : 1;

  # Both entry points are open statuses; an item may never be born resolved.
  $params->{resolution} = '';

  return;
}

sub config_add_panels {
  my ($self, $args) = @_;
  $args->{panel_modules}{IssueTypeWorkflow}
    = 'Bugzilla::Extension::IssueTypeWorkflow::Config';
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
