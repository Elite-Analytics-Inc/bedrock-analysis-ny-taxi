<script>
  import { EvidenceDefaultLayout } from "@evidence-dev/core-components";
  export let data;
</script>

<EvidenceDefaultLayout {data}
  hideSidebar={true}
  hideHeader={true}
  hideBreadcrumbs={true}
  hideTOC={true}
  fullWidth={false}
  builtWithEvidence={false}
  neverShowQueries={true}
>
  <slot slot="content" />
</EvidenceDefaultLayout>
