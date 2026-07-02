param(
  [string]$CustomModel = "gradient_boosting",
  [int]$MinTrainRows = 200,
  [string]$PredictionsOutput = "data\hf_daily_predictions.json",
  [switch]$UseInferenceApi
)

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

$cmd = @(
  "src\betting_bot.py",
  "--hf-daily-run",
  "--hf-custom-model", $CustomModel,
  "--hf-min-train-rows", "$MinTrainRows",
  "--hf-predictions-output", $PredictionsOutput
)

if ($UseInferenceApi) {
  $cmd += "--hf-predict-via-api"
}

python @cmd
