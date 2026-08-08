package Bugzilla::Extension::IssueTypeWorkflow::Config;

use 5.14.0;
use strict;
use warnings;

use Bugzilla::Config::Common;

our $sortkey = 5100;

sub get_param_list {
  my ($class) = @_;

  return (
    # The persistent "this installation has been provisioned" marker.
    #
    # It is what makes a missing piece of the model fail CLOSED. Without it the
    # extension could only ask "does the model look complete?", and an
    # administrator renaming one status through editvalues.cgi would silently
    # switch enforcement off for the whole installation.
    #
    # Off  -> bootstrap: the model is not in place yet, nothing is enforced.
    # On   -> the model MUST be complete; if anything is missing, every guarded
    #         change is refused until setup-projects.pl is re-run.
    #
    # setup-projects.pl turns it on at the end of a successful apply, and off
    # again only if you deliberately ask it to.
    {name => 'issue_type_workflow_enforced', type => 'b', default => 0},
  );
}

1;
