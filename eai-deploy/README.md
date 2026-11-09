# eai-deploy – Kubernetes configuration for eai-person-app (managed by Argo CD)

Folder of the `eai` repo (not a Maven module). Argo CD watches `eai-deploy/overlays/*` and applies changes.
Commands below are run from this folder (`/home/badr/git/eai/eai-deploy`).

```
base/            Deployment, Service, default ConfigMap (all env variables)
overlays/dev/    namespace eai-dev, DEBUG traces, dev endpoints, dev Secret
overlays/prod/   namespace eai-prod, 2 replicas, prod endpoints, Secret NOT in Git
argocd/          Argo CD Applications (dev = auto sync, prod = manual sync)
```

- Variables: ConfigMap `eai-person-app-config` (base + `behavior: merge` per env).
- Credentials: Secret `eai-person-app-secret`. Prod, created outside Git:
  `kubectl -n eai-prod create secret generic eai-person-app-secret --from-literal=DB_USER=... --from-literal=DB_PASSWORD=... --from-literal=ARTEMIS_USER=... --from-literal=ARTEMIS_PASSWORD=...`
- A ConfigMap change changes its generated name, so pods restart automatically.

Check: `kubectl kustomize overlays/dev` and `kubectl kustomize overlays/prod`.
Register in Argo CD: `kubectl apply -n argocd -f argocd/`.
Release: change `newTag` in the overlay, commit, push (prod: then Sync in Argo CD).
