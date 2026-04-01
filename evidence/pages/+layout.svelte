<script>
  import { EvidenceDefaultLayout } from "@evidence-dev/core-components";
  export let data;
</script>

<EvidenceDefaultLayout {data}
  title="Bedrock Analytics"
  hideSidebar={true}
  hideBreadcrumbs={true}
  hideTOC={true}
  fullWidth={true}
  builtWithEvidence={false}
  neverShowQueries={true}
>
  <slot slot="content" />
</EvidenceDefaultLayout>
