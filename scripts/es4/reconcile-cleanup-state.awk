# Reconcile a durable WIPING capture (first input) with the cluster's current
# Deployment list (second input).
#
# Existing names retain their ORIGINAL captured replica counts: they may be at
# zero because an interrupted cleanup already quiesced them. Deployments added
# since that capture use their current desired count, so they are fenced and
# restored by the resumed wipe. Deployments removed since capture disappear.
NR == FNR {
  if (FNR > 1) {
    captured[$1] = $2
  }
  next
}

NF >= 2 {
  name[++count] = $1
  current[$1] = $2
}

END {
  print "WIPING"
  for (i = 1; i <= count; i++) {
    deployment = name[i]
    replicas = (deployment in captured) ? captured[deployment] : current[deployment]
    print deployment, replicas
  }
}
