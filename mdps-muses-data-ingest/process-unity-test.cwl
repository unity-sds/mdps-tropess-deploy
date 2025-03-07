#!/usr/bin/env cwl-runner
arguments:
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
  collection_group_keyword:
    default: los_angeles
    type: string
  collection_version:
    default: 1
    type: int
  compress_data_files:
    default: true
    type: boolean
  input: Directory
  input_data_base_path:
    default: s3://tropess-temp/rs/muses/
    type: string
  input_data_ingest_path:
    default: CRIS-JPSS-1/Release_1.23.0/Los_Angeles/Products/2025/01/07
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
    dockerPull: 561555463819.dkr.ecr.us-west-2.amazonaws.com/tropess/mdps-muses-data-ingest:latest
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
