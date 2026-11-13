#!/usr/bin/env bash
# resolve-existing-web-image.sh — name the web image a deploy-without-build will ship.
#
# THE RACE THIS CLOSES. Jenkinsfile.web-service has two paths into its Deploy stage and until now
# only one of them bound the image:
#
#   * BUILD_IMAGE=true triggers options-edge-web-deploy under WEB_PERMITTED_SHA, reads THAT build's
#     image lock (fetch-permitted-image-lock.sh) and deploys exactly its digest as REQUIRED_IMAGE.
#   * BUILD_IMAGE=false — the DEFAULT — skipped that stage entirely, so REQUIRED_IMAGE stayed empty.
#     bind-required-image.sh reads an empty REQUIRED_IMAGE as "leave the mutable-tag pin unchanged",
#     and scripts/deploy/apply.sh then pins whatever the floating :dev (or :prod) tag resolves to AT
#     DEPLOY TIME. Any web build that pushed that tag in between silently decided what shipped.
#
# A WEB_PERMITTED_SHA handed to that second path was read by nothing at all. The job recorded a
# permitted commit (write-permission-receipt.sh) it had never compared against the image it shipped:
# a parameter that looks like it binds something and binds nothing is worse than no parameter, so
# this refuses rather than ignores (the same rule PERMITTED_SHA itself follows — nothing is
# substituted for a missing value).
#
# ONE ACCEPTED FORM PER INTENT, and everything else refused. Blacklisting the ways a caller can get
# this wrong has failed in this repository before; so the two forms below are the whitelist.
#
#   Form 1 — REVIEWED (preferred). WEB_PERMITTED_SHA + WEB_IMAGE_BUILD_NUMBER. Resolved through
#     fetch-permitted-image-lock.sh, unchanged: it verifies the lock belongs to that build of that
#     job, that the build was made from that commit, and that exactly one digest-pinned entry for
#     the image exists. The digest it prints is the one that deploys. This form carries a COMMIT
#     CLAIM: the deploy can say which source built what it shipped.
#
#   Form 2 — DIGEST. WEB_IMAGE_REF, itself digest-pinned. For redeploying the image already running
#     (a manifest-only change): read it off the pod. It is race-free — a digest names one image for
#     ever — but it carries NO COMMIT CLAIM, and the caller is told so, because a digest alone
#     cannot say what source produced it.
#
# WHY NOT A SINGLE FORM. Form 1 cannot express "whatever is running now" without a scan over the
# build history to find which build produced that digest; form 2 cannot express "the image reviewed
# as commit X" without trusting the caller to have done the lookup. Each is exact for its own
# question, and giving both is refused rather than resolved by preference.
#
# DRY RUN. DEPLOY_DRY_RUN=true changes nothing, so it does not have to name an image — the same
# exemption scripts/deploy/require-batch-image-binding.sh makes. But a dry run that pins the tag's
# CURRENT resolution is evidence about an image a later real deploy may not use, so when it is
# exempted that is said out loud on stderr rather than left for the reader to infer. An identity
# GIVEN on a dry run is still honoured, so a dry run can be made to describe exactly one image.
#
# Inputs (environment): BUILD_IMAGE, DEPLOY_DRY_RUN, WEB_PERMITTED_SHA, WEB_IMAGE_BUILD_NUMBER,
# WEB_IMAGE_REF, plus whatever fetch-permitted-image-lock.sh needs (JENKINS_URL, JOB_NAME) for form 1.
#
# stdout: EXACTLY one line, `<registry>/<image>:<tag>@sha256:<64 hex>` — or nothing at all, which
# means "no identity required on this path" and is only ever printed for an exempted dry run or for
# BUILD_IMAGE=true (where the Build stage mints the identity itself). Diagnostics go to stderr.
#
# PARAMETER VALIDATION HAPPENS BEFORE ANY NETWORK CALL, deliberately: every refusal below is
# reachable with no Jenkins controller, no registry and no credentials, which is what lets
# tests/test_web_deploy_image_binding.py execute them instead of pattern-matching a Jenkinsfile.
set -euo pipefail

JOB='options-edge-web-deploy'
IMAGE='options-edge-web'

refuse() {
  echo "resolve-existing-web-image: REFUSED — $*" >&2
  exit 1
}

# An unset variable and one set to the empty string mean the same thing here: Jenkins DROPS an
# environment entry whose value is empty, so a parameter left blank arrives as unset. Under `set -u`
# the two must not be told apart, or a blank parameter would crash instead of refusing.
build_image="${BUILD_IMAGE:-false}"
dry_run="${DEPLOY_DRY_RUN:-false}"
sha="${WEB_PERMITTED_SHA:-}"
build="${WEB_IMAGE_BUILD_NUMBER:-}"
ref="${WEB_IMAGE_REF:-}"

# The two booleans are read as STRINGS, and only the exact literals are accepted. A typo'd
# `DEPLOY_DRY_RUN=ture` read as false would turn an exempt dry run into a refusal (noisy, safe), but
# `BUILD_IMAGE=ture` read as false would demand an identity the build path was going to mint —
# whereas anything-but-true read as `true` would skip the requirement altogether. Neither guess is
# sound, so an unrecognised value refuses.
case "$build_image" in true|false) ;; *) refuse "BUILD_IMAGE must be exactly 'true' or 'false' (got '$build_image')" ;; esac
case "$dry_run" in true|false) ;; *) refuse "DEPLOY_DRY_RUN must be exactly 'true' or 'false' (got '$dry_run')" ;; esac

HOWTO_FORM1="WEB_PERMITTED_SHA + WEB_IMAGE_BUILD_NUMBER — the full 40-character options-edge commit
                 whose image you are deploying, and the $JOB build number that built it
                 (open that build in Jenkins; its number is in the URL)"
HOWTO_FORM2="WEB_IMAGE_REF — the digest-pinned reference of the image already running, e.g.
                 kubectl -n optionsedge get deploy/options-edge-web \\
                   -o jsonpath='{.spec.template.spec.containers[0].image}'
                 (it must already contain @sha256:…; a bare tag is refused)"

# ---- BUILD_IMAGE=true: the Build stage mints the identity from the child build it triggered ------
# Accepting an identity here as well would create a second, quieter answer to "what deploys", and
# the two could disagree. So they must be absent — not preferred over, not merged with.
if [ "$build_image" = true ]; then
  [ -z "$build" ] || refuse "BUILD_IMAGE=true mints its own image identity from the build it triggers, but WEB_IMAGE_BUILD_NUMBER='$build' was given. Clear it, or set BUILD_IMAGE=false to deploy an image that already exists."
  [ -z "$ref" ] || refuse "BUILD_IMAGE=true mints its own image identity from the build it triggers, but WEB_IMAGE_REF='$ref' was given. Clear it, or set BUILD_IMAGE=false to deploy an image that already exists."
  # WEB_PERMITTED_SHA is REQUIRED on that path and the Build stage checks it there, where the
  # refusal can also name the downstream job it is about to trigger. Nothing to print.
  exit 0
fi

# ---- BUILD_IMAGE=false: the deploy must name the image it ships ---------------------------------
# AMBIGUITY FIRST. Checked before the half-filled cases below, so that `WEB_IMAGE_REF` together with
# half of form 1 is reported as the contradiction it is rather than as a missing field.
if [ -n "$ref" ] && { [ -n "$sha" ] || [ -n "$build" ]; }; then
  refuse "both image forms were given and they are not merged: WEB_IMAGE_REF='$ref' names a digest directly, while WEB_PERMITTED_SHA='$sha'/WEB_IMAGE_BUILD_NUMBER='$build' asks for the image a reviewed build produced. Give exactly one — preferring the reviewed form, which carries a commit claim."
fi

if [ -z "$ref" ] && [ -z "$sha" ] && [ -z "$build" ]; then
  if [ "$dry_run" = true ]; then
    echo "resolve-existing-web-image: DEPLOY_DRY_RUN=true and no image identity given — nothing will be" >&2
    echo "  applied, so none is required. NOTE the render below pins whatever the MUTABLE tag resolves to" >&2
    echo "  right now; a later real deploy can resolve a different digest, so this run is not evidence" >&2
    echo "  about any particular image. Pass one of the forms above to make it so." >&2
    exit 0
  fi
  refuse "BUILD_IMAGE=false deploys an image that already exists, so this run must say WHICH one —
                 otherwise the deploy ships whatever the mutable tag happens to resolve to, which any
                 other web build can have moved. Give exactly one of:
               * $HOWTO_FORM1
               * $HOWTO_FORM2"
fi

# HALF-FILLED FORM 1. Each half is useless alone: a commit does not say which build produced the
# image, and a build number alone would deploy that build's image without checking what it was built
# from — which is the very substitution this script exists to prevent.
if [ -z "$ref" ]; then
  [ -n "$sha" ] || refuse "WEB_IMAGE_BUILD_NUMBER='$build' was given without WEB_PERMITTED_SHA. A build number alone would deploy that build's image without verifying the commit it was built from. Add the 40-character options-edge commit, or use WEB_IMAGE_REF to name a digest outright."
  [ -n "$build" ] || refuse "WEB_PERMITTED_SHA='$sha' was given without WEB_IMAGE_BUILD_NUMBER. A commit alone does not identify an image: the same commit can have been built many times, and the lock that proves what was built is a property of one BUILD. Add the $JOB build number, or use WEB_IMAGE_REF to name a digest outright."
fi

# ---- Form 2: a digest, named outright -----------------------------------------------------------
if [ -n "$ref" ]; then
  # The SAME shape the build path asserts on fetch-permitted-image-lock.sh's output, so both forms
  # hand the deploy an identically-shaped reference and bind-required-image.sh sees one contract.
  printf '%s' "$ref" | grep -Eq '^[^@[:space:]]+@sha256:[0-9a-f]{64}$' \
    || refuse "WEB_IMAGE_REF must be a digest-pinned reference, '<registry>/<repo>[:tag]@sha256:<64 lowercase hex>' (got '$ref'). A bare tag is exactly the moving target this parameter exists to avoid."
  echo "resolve-existing-web-image: deploying the digest named by WEB_IMAGE_REF. This carries NO commit" >&2
  echo "  claim — a digest alone does not say what source built it. Use WEB_PERMITTED_SHA +" >&2
  echo "  WEB_IMAGE_BUILD_NUMBER when the deploy should record which reviewed commit it shipped." >&2
  # ONLY THE DIGEST BINDS. bind-required-image.sh compares repository names with the tag stripped
  # (_bri_repo_of) and then returns `${PINNED_IMAGE%%@*}@$req_digest` — the RENDER's repo:tag with
  # this digest. So the tag written here never reaches the pod spec, and a tag that disagrees with
  # the digest (a build number copied from one build beside a digest from another) would change
  # nothing about what deploys while reading in the log as though it had. Say so, rather than let the
  # tag be mistaken for part of the binding.
  echo "  NOTE only the digest binds: the tag in this reference is informational and is replaced by" >&2
  echo "  the render's own tag before the apply. Naming a build's tag here does not pin that build." >&2
  printf '%s\n' "$ref"
  exit 0
fi

# ---- Form 1: the image a reviewed build produced ------------------------------------------------
# Shapes are checked HERE rather than left to fetch-permitted-image-lock.sh: that script refuses them
# too, but only after it has resolved the job path and reached the controller, and a malformed
# parameter should not need a network round trip to be rejected.
printf '%s' "$sha" | grep -Eq '^[0-9a-f]{40}$' \
  || refuse "WEB_PERMITTED_SHA must be a full 40-character lowercase commit id (got '$sha'). An abbreviated commit is refused: nothing is substituted or expanded for it."
printf '%s' "$build" | grep -Eq '^[0-9]+$' \
  || refuse "WEB_IMAGE_BUILD_NUMBER must be a plain build number (got '$build')."

# CHILD_BUILD_URL is the build path's cross-check that the job Jenkins ran is the job the resolver
# addressed. There is no child build on THIS path, so an inherited value could only disagree with
# the build being read and would refuse a correct request. Unset it for the call.
unset CHILD_BUILD_URL
exec env -u CHILD_BUILD_URL bash "$(dirname "${BASH_SOURCE[0]}")/fetch-permitted-image-lock.sh" \
  "$JOB" "$build" "$sha" "$IMAGE"
