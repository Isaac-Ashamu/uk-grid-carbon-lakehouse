{{ config(materialized='view') }}

{{ pivot_generation_mix(ref('stg_generation_national')) }}
