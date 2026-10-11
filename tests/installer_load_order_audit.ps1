param([string]$Source,[string]$AuditPath,[string]$Mutation='',[string]$LibraryRoot='')
$ErrorActionPreference='Stop'
$tokens=$null;$errors=$null
$text=[IO.File]::ReadAllText($Source)
$root=if($LibraryRoot){$LibraryRoot}else{Split-Path -Parent $Source}
$leafPaths=@{
    portable='kmtech_shared/powershell/portable.ps1'
    data='kmtech_shared/powershell/data_generation.ps1'
    bootstrap='tools/bootstrap_integrity.ps1'
    fence=if(Test-Path (Join-Path $root 'tools/container_writer_fence.ps1')){'tools/container_writer_fence.ps1'}else{'tools/label_writer_fence.ps1'}
}
$definitions=@{};$local=@{};$imports=[Collections.Generic.List[object]]::new()
$dominatingImports=@{};$bodyCommands=@{};$graphs=@{}
function Owner($Node){
    for($p=$Node.Parent;$null -ne $p;$p=$p.Parent){
        if($p -is [Management.Automation.Language.FunctionDefinitionAst]){return $p}
    }
    return $null
}
function Container($Node){
    for($p=$Node.Parent;$null -ne $p;$p=$p.Parent){
        if($p -is [Management.Automation.Language.StatementBlockAst] -or
           $p -is [Management.Automation.Language.NamedBlockAst]){return $p}
    }
}
function Leaf($Command){
    if($Command.InvocationOperator -ne [Management.Automation.Language.TokenKind]::Dot){return ''}
    $body=$Command.Extent.Text
    if($body -match 'data_generation\.ps1'){return 'data'}
    if($body -match 'writerFenceHelperPath|writer_fence\.ps1'){return 'fence'}
    if($body -match 'BootstrapIntegrityFunctions|bootstrap_integrity\.ps1'){return 'bootstrap'}
    if($body -match 'sharedLeaf|Get-(?:LabelSharedPortableLeafPath|BootstrapSharedPortableLeaf)'){return 'portable'}
    return ''
}
foreach($leaf in $leafPaths.Keys){
    $path=Join-Path $root $leafPaths[$leaf]
    $tree=[Management.Automation.Language.Parser]::ParseFile($path,[ref]$tokens,[ref]$errors)
    if($errors.Count){throw 'Shared PowerShell parse failed'}
    foreach($d in @($tree.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst]},$true))){
        if(-not $definitions.ContainsKey($d.Name)){$definitions[$d.Name]=@()}
        $definitions[$d.Name]+=,[pscustomobject]@{leaf=$leaf;definition=$d}
    }
}
if($Mutation){
    if($Mutation -eq 'data'){
        $pattern='(?m)^([ \t]*)(\. \(Get-BootstrapSharedPortableLeaf \$HelperRoot ''data_generation\.ps1''\))'
        if([regex]::Matches($text,$pattern).Count -ne 1){throw 'Nested data import was not classified'}
        $text=[regex]::Replace($text,$pattern,('$1Assert-KmtechDataIdentity'+[Environment]::NewLine+'$1$2'),1)
    }else{
    $name=if($Mutation -eq 'fence'){
        @($definitions.Keys | Where-Object {
            @($definitions[$_] | Where-Object {$_.leaf -eq 'fence'}).Count -gt 0
        } | Sort-Object)[0]
    }else{'ConvertTo-KmtechFullPath'}
    $text=[regex]::Replace($text,'(function Full\([^\r\n]+\)\s*\{)',('$1'+[Environment]::NewLine+$name),1)
    }
}
$ast=[Management.Automation.Language.Parser]::ParseInput($text,[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'Installer PowerShell parse failed'}
foreach($s in $ast.EndBlock.Statements){
    if($s -is [Management.Automation.Language.FunctionDefinitionAst]){$local[$s.Name]=$s}
}
$commands=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.CommandAst]},$true))
foreach($call in $commands){
    $leaf=Leaf $call
    if($leaf){$imports.Add([pscustomobject]@{node=$call;leaf=$leaf;owner=(Owner $call);container=(Container $call)})}
}
if(-not @($imports | Where-Object {$_.leaf -eq 'portable'}).Count){throw 'Portable import was not classified'}
if(-not @($imports | Where-Object {$_.leaf -eq 'data'}).Count){throw 'Data import was not classified'}
function Truth($Node,$Mode){
    if($Node -is [Management.Automation.Language.ParenExpressionAst]){return Truth $Node.Pipeline $Mode}
    if($Node -is [Management.Automation.Language.PipelineAst] -and $Node.PipelineElements.Count -eq 1){return Truth $Node.PipelineElements[0] $Mode}
    if($Node -is [Management.Automation.Language.CommandExpressionAst]){return Truth $Node.Expression $Mode}
    if($Node -is [Management.Automation.Language.VariableExpressionAst]){
        $name=$Node.VariablePath.UserPath
        if($name -eq 'true'){return 1};if($name -eq 'false'){return 0}
        if($Mode.ContainsKey($name)){return [int][bool]$Mode[$name]}
    }
    if($Node -is [Management.Automation.Language.UnaryExpressionAst] -and $Node.TokenKind -eq 'Not'){
        $value=Truth $Node.Child $Mode;if($value -ge 0){return 1-$value}
    }
    if($Node -is [Management.Automation.Language.BinaryExpressionAst]){
        $left=Truth $Node.Left $Mode;$right=Truth $Node.Right $Mode
        if($Node.Operator -eq 'And'){
            if($left -eq 0 -or $right -eq 0){return 0}
            if($left -eq 1 -and $right -eq 1){return 1}
        }
        if($Node.Operator -eq 'Or'){
            if($left -eq 1 -or $right -eq 1){return 1}
            if($left -eq 0 -and $right -eq 0){return 0}
        }
    }
    return -1 # Unknown conditions keep all possible branches.
}
function Active($Node,$Mode){
    $child=$Node;$p=$Node.Parent
    while($null -ne $p){
        if($p -is [Management.Automation.Language.IfStatementAst]){
            $earlierTrue=$false
            foreach($clause in $p.Clauses){
                $truth=Truth $clause.Item1 $Mode
                if([object]::ReferenceEquals($child,$clause.Item2)){
                    if($earlierTrue -or $truth -eq 0){return $false}
                    break
                }
                if($truth -eq 1){$earlierTrue=$true}
            }
            if([object]::ReferenceEquals($child,$p.ElseClause) -and $earlierTrue){return $false}
        }
        $child=$p;$p=$p.Parent
    }
    return $true
}
function Available($Node,$Inherited,$Mode){
    $loaded=@{};foreach($leaf in $Inherited.Keys){$loaded[$leaf]=$true}
    if(-not $dominatingImports.ContainsKey($Node)){
        $ancestors=@();for($p=$Node.Parent;$null -ne $p;$p=$p.Parent){$ancestors+=,$p}
        $dominatingImports[$Node]=@($imports | Where-Object {
            $_.node.Extent.EndOffset -le $Node.Extent.StartOffset -and
            $ancestors -contains $_.container
        })
    }
    foreach($import in $dominatingImports[$Node]){
        if(Active $import.node $Mode){$loaded[$import.leaf]=$true}
    }
    return $loaded
}
function Graph($Seeds,$Mode,$Boundary,[bool]$DeferredOwner){
    $pending=[Collections.Generic.Queue[object]]::new();$visited=@{};$functions=@{};$dependencies=@{}
    foreach($seed in $Seeds){$pending.Enqueue([pscustomobject]@{node=$seed;loaded=@{}})}
    while($pending.Count){
        $item=$pending.Dequeue();$call=$item.node;$name=$call.GetCommandName()
        if(-not $name){continue}
        $loaded=Available $call $item.loaded $Mode
        $key=$name+'|'+(($loaded.Keys | Sort-Object)-join ',')
        if($visited.ContainsKey($key)){continue};$visited[$key]=$true
        $definition=$null
        if($local.ContainsKey($name)){
            $definition=$local[$name];$functions[$name]=$true
            if(-not $DeferredOwner -and $definition.Extent.StartOffset -ge $Boundary){
                $dependencies[$name]='later-local-definition';continue
            }
        }elseif($definitions.ContainsKey($name)){
            $admitted=@($definitions[$name] | Where-Object {$loaded.ContainsKey($_.leaf)})
            if(-not $admitted.Count){$dependencies[$name]='unloaded-shared-function';continue}
            $definition=$admitted[-1].definition
        }
        if($null -ne $definition){
            if(-not $bodyCommands.ContainsKey($definition)){
                $bodyCommands[$definition]=@($definition.Body.FindAll({param($n)$n -is [Management.Automation.Language.CommandAst]},$true))
            }
            foreach($next in $bodyCommands[$definition]){
                $pending.Enqueue([pscustomobject]@{node=$next;loaded=$loaded})
            }
        }
    }
    return @{functions=@($functions.Keys | Sort-Object);dependencies=@($dependencies.Keys | Sort-Object)}
}
$scenarios=@(
    @{name='no-arguments'},
    @{name='plan-only';PlanOnly=$true},
    @{name='worker-inventory';WorkerAccount=$true;WorkerSurveyInventory=$true;workerArgumentsSupplied=$true},
    @{name='worker-evidence';WorkerAccount=$true;WorkerSurveyInventory=$true;EvidencePath=$true;workerArgumentsSupplied=$true},
    @{name='worker-evidence-plan-only';WorkerAccount=$true;WorkerSurveyInventory=$true;EvidencePath=$true;PlanOnly=$true;workerArgumentsSupplied=$true},
    @{name='machine-recovery';WorkerAccount=$true;WorkerSurveyInventory=$true;RecoverMachineInstallation=$true;MachineRecoveryAuditPath=$true;MachineRecoveryAuditSha256=$true;workerArgumentsSupplied=$true},
    @{name='restore';RestoreVerifiedReplacement=$true;RestoreReceiptPath=$true;RestoreReceiptSha256=$true},
    @{name='retired-removal';CompleteRetiredRemoval=$true;Uninstall=$true;OldSourceRoot=$true;RetiredRemovalReceiptSha256=$true;RetiredStopMarkerSha256=$true},
    @{name='rollback-resume';ResumeRollbackAuditPath=$true;ResumeRollbackAuditSha256=$true}
)
$rows=@()
foreach($scenario in $scenarios){
    $mode=@{}
    foreach($parameter in $ast.ParamBlock.Parameters){$mode[$parameter.Name.VariablePath.UserPath]=$false}
    foreach($name in $scenario.Keys){$mode[$name]=$scenario[$name]}
    foreach($import in $imports){
        if(-not (Active $import.node $mode)){continue}
        $owner=$import.owner
        $seeds=@($commands | Where-Object {
            ($_.Extent.StartOffset -lt $import.node.Extent.StartOffset -or
             ($_.Extent.StartOffset -gt $import.node.Extent.StartOffset -and $_.Extent.EndOffset -le $import.node.Extent.EndOffset)) -and
            [object]::ReferenceEquals((Owner $_),$owner) -and (Active $_ $mode)
        })
        $signature=[string]$import.node.Extent.StartOffset+'|'+(($seeds | ForEach-Object {$_.Extent.StartOffset})-join ',')+'|'+
            (($imports | Where-Object {Active $_.node $mode} | ForEach-Object {$_.node.Extent.StartOffset})-join ',')
        if(-not $graphs.ContainsKey($signature)){$graphs[$signature]=Graph $seeds $mode $import.node.Extent.StartOffset ($null -ne $owner)}
        $graph=$graphs[$signature]
        $rows+=,@{scenario=$scenario.name;leaf=$import.leaf;line=$import.node.Extent.StartLineNumber;
            owner=if($owner){$owner.Name}else{'script'};functions=$graph.functions;dependencies=$graph.dependencies}
    }
}
@{engine_version=$PSVersionTable.PSVersion.ToString();frontiers=$rows;mutation=$Mutation} |
    ConvertTo-Json -Depth 7 | Set-Content -LiteralPath $AuditPath -Encoding UTF8
if(@($rows | Where-Object {$_.dependencies.Count}).Count){exit 1}
exit 0
