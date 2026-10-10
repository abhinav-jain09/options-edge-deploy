#!/usr/bin/perl
# Converge the REQ-5d parameters on EVERY boot.
#
# Why this exists: the checksetup answers file is NOT a convergence mechanism. Verified in source at
# the pinned commit — Bugzilla/Config.pm's update_params() consults the answers hash only inside
# `unless (exists $param->{$name})` (Config.pm:121-131 @ 276673ab6). So an answer applies on FIRST
# boot, when the param is absent from data/params.json, and is ignored on every boot after that.
#
# Without this script, a param changed by hand in the admin UI — or a value we later change in the
# template — would silently never converge, and the REQ-5d postconditions would drift from the
# design with no signal. This makes the template the actual source of truth it claims to be.
#
# Idempotent: it only writes when a value differs, and prints what it changed so the container log
# carries the evidence.
use 5.14.0;
use strict;
use warnings;

use lib qw(. lib);
use Bugzilla;
use Bugzilla::Config qw(SetParam write_params);

# The REQ-5d contract. Keep in lockstep with oidc/checksetup-answers.tmpl: the template seeds a
# fresh install, this converges an existing one, and they must not disagree.
my %REQUIRED = (
  user_info_class            => 'Env,CGI',
  auth_env_id                => 'OIDC_CLAIM_sub',
  auth_env_email             => 'OIDC_CLAIM_email',
  auth_env_realname          => 'OIDC_CLAIM_name',
  requirelogin               => 1,
  createemailregexp          => '',
  maxlocalattachment         => 0,
  maxattachmentsize          => 10240,
  usevisibilitygroups        => 1,
  insidergroup               => '',
  useqacontact               => 0,
  usetargetmilestone         => 0,
  letsubmitterchoosepriority => 0,
  urlbase                    => $ENV{BZ_URLBASE} // '',
);

my $changed = 0;
foreach my $name (sort keys %REQUIRED) {
  my $want = $REQUIRED{$name};
  next if $name eq 'urlbase' && $want eq '';   # never blank a urlbase from a missing env var

  my $have = Bugzilla->params->{$name};
  $have = '' unless defined $have;

  if ("$have" ne "$want") {
    print "[portal] param $name: '$have' -> '$want'\n";
    SetParam($name, $want);
    $changed = 1;
  }
}

if ($changed) {
  write_params();
  print "[portal] REQ-5d parameters converged\n";
}
else {
  print "[portal] REQ-5d parameters already match the contract\n";
}
