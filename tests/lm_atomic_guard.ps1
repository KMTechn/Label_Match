param([string]$HostRoot,[string]$SourceFile='')
$ErrorActionPreference='Stop'
function Get-AtomicPublicationSites([string]$Source) {
    $tokens=$null;$errors=$null
    $tree=[Management.Automation.Language.Parser]::ParseInput($Source,[ref]$tokens,[ref]$errors)
    if($errors.Count){throw 'ATOMIC_GUARD_PARSE'}
    foreach($node in $tree.FindAll({param($n)
        if($n -is [Management.Automation.Language.CommandAst]){
            return ($n.GetCommandName() -replace '^.*\\','') -in @('Move-Item','Rename-Item','mv','mi','ren','rni')
        }
        if($n -isnot [Management.Automation.Language.InvokeMemberExpressionAst]){return $false}
        if($n.Member.Value -ieq 'MoveFileExW'){return $true}
        return $n.Expression -is [Management.Automation.Language.TypeExpressionAst] -and
            $n.Expression.TypeName.FullName -match '^(System\.)?IO\.File$' -and $n.Member.Value -in @('Move','Replace')
    },$true)){
        $parent=$node.Parent;$function=''
        while($parent){
            if($parent -is [Management.Automation.Language.FunctionDefinitionAst]){$function=$parent.Name;break}
            $parent=$parent.Parent
        }
        $hash=[Security.Cryptography.SHA256]::Create()
        try{$sha=[BitConverter]::ToString($hash.ComputeHash([Text.Encoding]::UTF8.GetBytes($node.Extent.Text))).Replace('-','').ToLowerInvariant()}
        finally{$hash.Dispose()}
        [pscustomobject]@{function=$function;line=$node.Extent.StartLineNumber;sha256=$sha}
    }
}
if($SourceFile){
    $sites=@(Get-AtomicPublicationSites ([IO.File]::ReadAllText($SourceFile)))
    @{sites=$sites.Count} | ConvertTo-Json -Compress
    if($sites.Count){exit 3}
    exit 0
}
$exceptions=Get-Content -LiteralPath (Join-Path $HostRoot 'tests/fixtures/lm_atomic_directory_sites.json') -Raw | ConvertFrom-Json
$used=@{};$forbidden=@()
$sharedExclusions = @{
    'kmtech_shared/powershell/data_generation.ps1' = '9e31c5aca69a410546b42d269e464b5848ef0e95f8d243010332f0458103ebf8'
    'kmtech_shared/powershell/portable.ps1' = 'eba0b2d69b831d386d2668e5605e63807d27612b877115a53a038a8b283dbb70'
    'kmtech_shared/powershell/ps51_module_prologue.ps1' = '9c5e083b8e332573f3368be3e9592a2e2409aa89891e15942026361be17d0b71'
}
foreach($file in @(& git -C $HostRoot ls-files --cached --others --exclude-standard '*.ps1' '*.psm1')){
    if($file.StartsWith('tests/') -or $sharedExclusions.ContainsKey($file)){if ($sharedExclusions.ContainsKey($file) -and (Get-FileHash -LiteralPath (Join-Path $HostRoot $file) -Algorithm SHA256).Hash.ToLowerInvariant() -cne $sharedExclusions[$file]) { throw 'SHARED_ATOMIC_EXCLUSION_CHANGED' }; continue}
    foreach($site in @(Get-AtomicPublicationSites ([IO.File]::ReadAllText((Join-Path $HostRoot $file))))){
        if($file -ceq 'INSTALL_THIS_PC.ps1' -and $site.function -ceq 'Invoke-LabelAtomicReplace'){continue}
        $match=@($exceptions | Where-Object {$_.file -ceq $file -and $_.function -ceq $site.function -and $_.sha256 -ceq $site.sha256})
        if($match.Count -ne 1){$forbidden+=@{file=$file;line=$site.line};continue}
        $key=$file+'|'+$site.function+'|'+$site.sha256
        $used[$key]=1+[int]$used[$key]
    }
}
if($forbidden.Count -or $used.Count -ne @($exceptions).Count){throw 'DIRECT_FILE_PUBLICATION_OR_DIRECTORY_EXCEPTION_DRIFT'}
foreach($entry in $exceptions){
    $key=$entry.file+'|'+$entry.function+'|'+$entry.sha256
    if($used[$key] -ne $entry.count){throw 'DIRECTORY_EXCEPTION_COUNT_DRIFT'}
}
@{status='PASS';directory_exceptions=$used.Count} | ConvertTo-Json -Compress
