param(
  [string]$CustomModel = "gradient_boosting",
  [int]$DaysBack = 365,
  [int]$MinTrainRows = 200
)

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

python src\betting_bot.py `
  --hf-bootstrap `
  --hf-days-back $DaysBack `
  --hf-retrain-publish `
  --hf-custom-model $CustomModel `
  --hf-min-train-rows $MinTrainRows
