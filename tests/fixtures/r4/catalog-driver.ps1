param([string]$Repo,[string]$Output)
$ErrorActionPreference='Stop'
$ExtractedFunctionNames=@('Get-LabelOfficialLegacyTaskExpectations')
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile((Join-Path $Repo 'INSTALL_THIS_PC.ps1'),[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'R4 product parse failed'}
$functions=@{}
foreach($node in $ast.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst]},$true)){$functions[$node.Name]=$node.Extent.Text}
if(-not $functions.ContainsKey($ExtractedFunctionNames[0])){
    [ordered]@{seam_available=$false} | ConvertTo-Json | Set-Content -LiteralPath $Output -Encoding UTF8
    exit 0
}
foreach($definition in $functions.Values){. ([scriptblock]::Create($definition))}
$env:COMPUTERNAME='R4-FIXTURE-PC';$env:SystemRoot='C:\Windows';$env:ProgramData='C:\ProgramData'
function Get-ItemProperty { return [pscustomobject]@{MachineGuid='R4_SYNTHETIC_MACHINE'} }
function Get-ScheduledTask {throw 'R4 catalog inspection must not touch scheduler'}
function New-Object {throw 'R4 catalog inspection must not create native objects'}
$entries=@(Get-LabelOfficialLegacyTaskExpectations)
$fingerprints=@()
foreach($entry in $entries){
    $hash=[Security.Cryptography.SHA256]::Create()
    try{
        $lines=@();$fields=[ordered]@{}
        foreach($name in @($entry.Keys | Sort-Object)){
            $line=$name+'='+[string]$entry[$name]+[char]10;$lines+=$line
            $fields[$name]=([BitConverter]::ToString($hash.ComputeHash([Text.Encoding]::UTF8.GetBytes($line)))).Replace('-','').ToLowerInvariant()
        }
        $full=([BitConverter]::ToString($hash.ComputeHash([Text.Encoding]::UTF8.GetBytes(($lines -join ''))))).Replace('-','').ToLowerInvariant()
        $fingerprints += [ordered]@{full_fingerprint=$full;field_fingerprints=$fields}
    }finally{$hash.Dispose()}
}
[ordered]@{seam_available=$true;entries=$fingerprints} | ConvertTo-Json -Depth 12 |
    Set-Content -LiteralPath $Output -Encoding UTF8
