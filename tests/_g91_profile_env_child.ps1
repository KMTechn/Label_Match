param([string]$Repo, [string]$Root, [string]$Output)
$ErrorActionPreference = 'Stop'
$installer = [IO.File]::ReadAllText((Join-Path $Repo 'INSTALL_CANONICAL_PORTABLE.ps1'))
$tokens = $null; $errors = $null
$tree = [Management.Automation.Language.Parser]::ParseInput($installer, [ref]$tokens, [ref]$errors)
if ($errors.Count) { throw 'Installer syntax error' }
$definitions = $tree.FindAll({ param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] }, $false)
foreach ($name in @('Get-LabelPathCompareKey', 'Resolve-LabelPathLinkTargets', 'Test-LabelLegacySharedProfilePath', 'Get-LabelAppProfileEnvValue')) {
    $definition = @($definitions | Where-Object Name -eq $name)
    if ($definition.Count -ne 1) { throw "missing $name" }
    . ([ScriptBlock]::Create($definition[0].Extent.Text))
}
$programData = Join-Path $Root 'ProgramData'
$shared = Join-Path $programData 'KMTech\Logistics'
[void][IO.Directory]::CreateDirectory((Join-Path $shared 'secrets'))
$env:PROGRAMDATA = $programData
$link = Join-Path $Root 'junction-to-shared'
New-Item -ItemType Junction -Path $link -Target $shared | Out-Null
$profileFile = Join-Path $shared 'runtime-profile.json'
$cases = [ordered]@{
    exact = $profileFile
    case = $profileFile.ToUpperInvariant()
    slash = $profileFile.Replace('\', '/')
    trailing = $profileFile + '\'
    verbatim = '\\?\' + $profileFile
    secret = Join-Path $shared 'secrets\bearer-token.dpapi'
    junction = Join-Path $link 'runtime-profile.json'
    sibling = Join-Path $programData 'KMTech\Logistics2\runtime-profile.json'
    appspecific = Join-Path $shared 'profiles\Label_Match\runtime-profile.json'
    secretsx = Join-Path $shared 'secretsX\a.dpapi'
}
$result = [ordered]@{}
foreach ($key in $cases.Keys) {
    $env:LABEL_MATCH_LOGISTICS_PROFILE_PATH = $cases[$key]
    $result[$key] = [ordered]@{
        legacy = [bool](Test-LabelLegacySharedProfilePath $cases[$key])
        value = [string](Get-LabelAppProfileEnvValue 6>$null)
    }
}
$env:LABEL_MATCH_LOGISTICS_PROFILE_PATH = ''
$result['unset'] = [ordered]@{ legacy = $false; value = [string](Get-LabelAppProfileEnvValue 6>$null) }
$result['cases'] = $cases
[IO.File]::WriteAllText($Output, ($result | ConvertTo-Json -Depth 4), (New-Object Text.UTF8Encoding($false)))
