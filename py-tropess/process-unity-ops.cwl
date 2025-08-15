#!/usr/bin/env cwl-runner
arguments:
- -p
- input_stac_catalog_dir
- $(inputs.input.path)
- -p
- output_stac_catalog_dir
- $(runtime.outdir)
baseCommand:
- papermill
- /home/jovyan/process.ipynb
- --cwd
- /home/jovyan
- output_nb.ipynb
- -f
- /tmp/inputs.json
- --log-output
- -k
- python3
class: CommandLineTool
cwlVersion: v1.2
inputs:
  granule_version:
    default: 2
    type: int
  input: Directory
  processing_species:
    default: null
    type: 'null'
  product_type:
    default: standard
    type: string
outputs:
  output:
    outputBinding:
      glob: $(runtime.outdir)
    type: Directory
  process_output_nb:
    outputBinding:
      glob: $(runtime.outdir)/output_nb.ipynb
    type: File
requirements:
  DockerRequirement:
    dockerPull: 103739919403.dkr.ecr.us-west-2.amazonaws.com/tropess/py-tropess:1.2.0
  InitialWorkDirRequirement:
    listing:
    - entry: $(inputs)
      entryname: /tmp/inputs.json
  InlineJavascriptRequirement: {}
  InplaceUpdateRequirement:
    inplaceUpdate: true
  NetworkAccess:
    networkAccess: true
  ShellCommandRequirement: {}
