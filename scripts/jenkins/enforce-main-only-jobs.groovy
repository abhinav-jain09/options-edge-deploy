import hudson.model.AbstractProject
import hudson.model.Job
import hudson.plugins.git.GitSCM
import jenkins.model.Jenkins
import org.jenkinsci.plugins.workflow.cps.CpsScmFlowDefinition
import org.jenkinsci.plugins.workflow.job.WorkflowJob

Jenkins jenkins = Jenkins.instance
List<String> changed = []

def disableJob = { Job job, String reason ->
  if (job instanceof WorkflowJob) {
    if (!job.disabled) {
      job.setDisabled(true)
      changed << "disabled ${job.fullName}: ${reason}"
    }
    job.description = "DISABLED: main-branch-only Jenkins deployment policy. ${reason}"
    job.save()
  } else if (job instanceof AbstractProject) {
    if (!job.disabled) {
      job.disable()
      changed << "disabled ${job.fullName}: ${reason}"
    }
    job.description = "DISABLED: main-branch-only Jenkins deployment policy. ${reason}"
    job.save()
  }
}

jenkins.getAllItems(Job).each { Job job ->
  if (job.fullName.contains(".bak.")) {
    disableJob(job, "backup deploy job is not allowed to run")
    return
  }

  if (job instanceof WorkflowJob) {
    def definition = job.definition
    if (definition instanceof CpsScmFlowDefinition && definition.scm instanceof GitSCM) {
      List<String> branches = definition.scm.branches.collect { it.name }
      if (branches.any { it != "*/main" }) {
        disableJob(job, "SCM branch spec is ${branches.join(",")}, only */main is allowed")
      }
    }
  }
}

jenkins.save()

println("Jenkins main-only deployment enforcement applied")
if (changed.isEmpty()) {
  println("No jobs needed a disabled-state change")
} else {
  changed.sort().each { println(it) }
}
