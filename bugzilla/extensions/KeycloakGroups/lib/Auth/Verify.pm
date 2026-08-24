package Bugzilla::Extension::KeycloakGroups::Auth::Verify;

use 5.14.0;
use strict;
use warnings;

use parent qw(Bugzilla::Auth::Verify::DB);

use Bugzilla::Constants;
use Bugzilla::Group;

# Which Bugzilla groups this extension is allowed to touch.
#
# This is the safety boundary and it is deliberately narrow: only per-application access groups
# named like `optionsedge-internal` or `fullfunding-external`. Bugzilla's privilege groups
# (admin, editbugs, tweakparams, ...) can never match, so a bad or empty claim can never strip
# someone's administrative rights — the worst case is losing access to bug products, which is
# recoverable by fixing the claim and logging in again.
use constant MANAGED_GROUP_RE => qr/^[A-Za-z0-9_]+-(?:internal|external)\z/;

# The claim carries full Keycloak paths, e.g. `/optionsedge/internal`.
# `/optionsedge/internal` -> `optionsedge-internal`
sub _claim_to_group_name {
  my ($path) = @_;
  return undef unless defined $path;
  $path =~ s/\A\s+|\s+\z//g;
  return undef if $path eq '';
  $path =~ s{\A/}{};
  $path =~ s{/}{-}g;
  return $path;
}

sub _claimed_group_names {
  # mod_auth_openidc flattens a multi-valued claim into one environment variable, joined by
  # OIDCClaimDelimiter (default ","). Both shapes are handled.
  my $raw = $ENV{'OIDC_CLAIM_groups'};
  return () unless defined $raw && $raw ne '';
  my @names;
  for my $part (split /,/, $raw) {
    my $name = _claim_to_group_name($part);
    push @names, $name if defined $name;
  }
  return @names;
}

sub create_or_update_user {
  my ($self, $params) = @_;

  my $result = $self->SUPER::create_or_update_user($params);

  # Never let a sync problem block a login: the user still gets in, with whatever groups they
  # already had, and the failure is recorded in the Apache error log.
  eval { _sync_groups($result->{user}) if $result && !$result->{failure} && $result->{user}; 1 }
    or warn "KeycloakGroups: group sync skipped: $@";

  return $result;
}

sub _sync_groups {
  my ($user) = @_;
  return unless $user && $user->id;

  my $dbh = Bugzilla->dbh;

  # Only groups that BOTH match the managed pattern AND exist in Bugzilla are considered.
  # A claim naming a group that does not exist is ignored rather than auto-creating one —
  # creating access groups is an administrative act, not something a token should trigger.
  my %managed;
  for my $group (@{ Bugzilla::Group->match({}) }) {
    $managed{$group->name} = $group->id if $group->name =~ MANAGED_GROUP_RE;
  }
  return unless %managed;

  my %want;
  for my $name (_claimed_group_names()) {
    $want{$name} = $managed{$name} if exists $managed{$name};
  }

  # Current DIRECT memberships, restricted to the managed set. Indirect memberships (the
  # internal -> external nesting) are derived by Bugzilla and must not be written here.
  my $rows = $dbh->selectall_arrayref(
    "SELECT g.name, g.id FROM user_group_map m JOIN groups g ON g.id = m.group_id
      WHERE m.user_id = ? AND m.grant_type = ? AND m.isbless = 0",
    undef, $user->id, GRANT_DIRECT);
  my %have;
  for my $row (@$rows) {
    $have{$row->[0]} = $row->[1] if exists $managed{$row->[0]};
  }

  my @add    = grep { !exists $have{$_} } keys %want;
  my @remove = grep { !exists $want{$_} } keys %have;
  return unless @add || @remove;

  for my $name (@add) {
    $dbh->do(
      "INSERT INTO user_group_map (user_id, group_id, isbless, grant_type) VALUES (?, ?, 0, ?)",
      undef, $user->id, $want{$name}, GRANT_DIRECT);
  }
  for my $name (@remove) {
    $dbh->do(
      "DELETE FROM user_group_map WHERE user_id = ? AND group_id = ? AND isbless = 0 AND grant_type = ?",
      undef, $user->id, $have{$name}, GRANT_DIRECT);
  }

  # Drop the cached group lists so this request already sees the new membership.
  delete $user->{groups};
  delete $user->{groups_direct};
  delete $user->{derived_regexp_groups};

  warn sprintf("KeycloakGroups: %s groups synced (added: %s; removed: %s)",
               $user->login,
               (@add    ? join(',', sort @add)    : 'none'),
               (@remove ? join(',', sort @remove) : 'none'));

  return;
}

1;
