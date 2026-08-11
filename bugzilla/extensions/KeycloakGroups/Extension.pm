# Keycloak is the single place where access is granted.
#
# Bugzilla's Env login can only read three things from the environment (auth_env_id,
# auth_env_email, auth_env_realname) — there is no auth_env_group, so group membership cannot
# arrive that way. This extension closes that gap: on every login it reads the `groups` claim
# that mod_auth_openidc puts in the environment and makes the user's Bugzilla groups match.
#
# Why the Verify class and not the Login class: Bugzilla::Auth::Login::Env declares
# requires_verification => 0, so Bugzilla::Auth::login() takes the else-branch and calls
# create_or_update_user() on the verifier. That is the first point at which the Bugzilla::User
# object exists (created or found), which is what group membership has to be applied to.
package Bugzilla::Extension::KeycloakGroups;

use 5.14.0;
use strict;
use warnings;

use parent qw(Bugzilla::Extension);

our $VERSION = '1.0';

sub auth_verify_methods {
  my ($self, $args) = @_;
  my $modules = $args->{modules};

  # The hook forbids ADDING keys — only an already-configured method may be overridden.
  # user_verify_class is `DB`, so that is the one we extend. If the parameter is ever
  # changed, this override simply stops applying rather than breaking login.
  if (exists $modules->{DB}) {
    $modules->{DB} = 'Bugzilla/Extension/KeycloakGroups/Auth/Verify.pm';
  }
}

__PACKAGE__->NAME;
