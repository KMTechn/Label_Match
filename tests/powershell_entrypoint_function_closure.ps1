param([string]$Root, [string]$EntriesPath, [string]$ResultPath)
$ErrorActionPreference = 'Stop'

# PS 5.1 engine / inbox Microsoft.PowerShell cmdlets. This fixed set must not
# depend on the developer's profile, aliases, installed modules or functions.
$Inbox = @{
    'Add-Member' = 'PS5.1 Utility: object members'
    'Add-Type' = 'PS5.1 Utility: CLR definitions'
    'ConvertFrom-Json' = 'PS5.1 Utility: JSON input'
    'ConvertTo-Json' = 'PS5.1 Utility: JSON output'
    'Copy-Item' = 'PS5.1 Management: file copying'
    'ForEach-Object' = 'PS5.1 Core: pipeline iteration'
    'Get-Acl' = 'PS5.1 Security: ACL inspection'
    'Get-AuthenticodeSignature' = 'PS5.1 Security: signature inspection'
    'Get-ChildItem' = 'PS5.1 Management: directory enumeration'
    'Get-Command' = 'PS5.1 Core: command inspection'
    'Get-Content' = 'PS5.1 Management: file reading'
    'Get-Date' = 'PS5.1 Utility: clock reading'
    'Get-FileHash' = 'PS5.1 Utility: file hashing'
    'Get-Item' = 'PS5.1 Management: item inspection'
    'Get-ItemProperty' = 'PS5.1 Management: registry inspection'
    'Get-Location' = 'PS5.1 Management: current directory'
    'Get-Process' = 'PS5.1 Management: process inspection'
    'Get-Variable' = 'PS5.1 Utility: local scope inspection'
    'Import-Module' = 'PS5.1 Core: module loading'
    'Join-Path' = 'PS5.1 Management: path composition'
    'Measure-Object' = 'PS5.1 Utility: aggregation'
    'Move-Item' = 'PS5.1 Management: file movement'
    'New-Item' = 'PS5.1 Management: item creation'
    'New-Object' = 'PS5.1 Utility: CLR object construction'
    'Out-File' = 'PS5.1 Utility: file output'
    'Out-Null' = 'PS5.1 Core: output suppression'
    'Remove-Item' = 'PS5.1 Management: item removal'
    'Resolve-Path' = 'PS5.1 Management: path resolution'
    'Select-Object' = 'PS5.1 Utility: projection'
    'Set-Acl' = 'PS5.1 Security: ACL publication'
    'Set-Content' = 'PS5.1 Management: file publication'
    'Set-Location' = 'PS5.1 Management: directory selection'
    'Set-StrictMode' = 'PS5.1 Core: language mode'
    'Set-Variable' = 'PS5.1 Utility: scope variables'
    'Sort-Object' = 'PS5.1 Utility: ordering'
    'Split-Path' = 'PS5.1 Management: path decomposition'
    'Start-Process' = 'PS5.1 Management: process launch'
    'Start-Sleep' = 'PS5.1 Utility: waiting'
    'Test-Path' = 'PS5.1 Management: path inspection'
    'Where-Object' = 'PS5.1 Core: pipeline filtering'
    'Write-Output' = 'PS5.1 Utility: success output'
    'Write-Warning' = 'PS5.1 Utility: warning output'
}
# Explicit platform allowances: names and reasons, never a Verb-* wildcard.
$Allowed = @{
    'Disable-ScheduledTask' = 'Windows ScheduledTasks: pause an owned startup task'
    'Enable-ScheduledTask' = 'Windows ScheduledTasks: restore an owned startup task'
    'Export-ScheduledTask' = 'Windows ScheduledTasks: inspect an owned task definition'
    'Get-ScheduledTask' = 'Windows ScheduledTasks: inspect startup task state'
    'Register-ScheduledTask' = 'Windows ScheduledTasks: publish an owned startup task'
    'Start-ScheduledTask' = 'Windows ScheduledTasks: start an owned startup task'
    'Unregister-ScheduledTask' = 'Windows ScheduledTasks: remove an owned startup task'
    'Get-LocalUser' = 'Windows LocalAccounts: inspect the designated worker account'
    'Get-CimInstance' = 'Windows CimCmdlets: inspect process and session identity'
    'Invoke-CimMethod' = 'Windows CimCmdlets: inspect/control the verified process chain'
}

function Parse-Source([string]$File) {
    $tokens = $null; $errors = $null
    $tree = [Management.Automation.Language.Parser]::ParseFile(
        (Join-Path $Root $File), [ref]$tokens, [ref]$errors)
    if ($errors.Count) { throw 'G62 source parse failed' }
    return $tree
}

function Get-Scope($Node) {
    for ($p = $Node.Parent; $null -ne $p; $p = $p.Parent) {
        if ($p -is [Management.Automation.Language.FunctionDefinitionAst]) { return $p }
    }
    return $null
}

function Resolve-StaticPath($Expression, $FileTree, $Seen) {
    $commands = @($Expression.FindAll({param($n)
        $n -is [Management.Automation.Language.CommandAst]
    }, $true) | ForEach-Object { $_.GetCommandName() })
    $strings = @($Expression.FindAll({param($n)
        $n -is [Management.Automation.Language.StringConstantExpressionAst]
    }, $true) | ForEach-Object { $_.Value })
    if ($commands -contains 'Get-LabelSharedPortableLeafPath') {
        $leaf = @($strings | Where-Object { $_ -match '\.ps1$' })
        if (-not $leaf.Count) { $leaf = @('portable.ps1') }
        foreach ($name in $leaf) { 'kmtech_shared/powershell/' + $name }
        return
    }
    $resolved=$false
    foreach ($s in $strings) {
        if ($s -notmatch '\.ps(m)?1$') { continue }
        $relative=$s.Replace('\','/') -replace '^app/',''
        foreach ($candidate in @($relative, ('tools/' + $relative))) {
            if (Test-Path -LiteralPath (Join-Path $Root $candidate) -PathType Leaf) {
                $candidate
                $resolved=$true
                break
            }
        }
    }
    if ($resolved) { return }
    foreach ($v in $Expression.FindAll({param($n)
        $n -is [Management.Automation.Language.VariableExpressionAst]
    }, $true)) {
        $name=$v.VariablePath.UserPath
        if ($Seen.ContainsKey($name)) { continue }
        $Seen[$name]=$true
        foreach ($assignment in $FileTree.FindAll({param($n)
            $n -is [Management.Automation.Language.AssignmentStatementAst] -and
            $n.Left.Extent.Text -ieq ('$' + $name)
        }, $true)) { Resolve-StaticPath $assignment.Right $FileTree $Seen }
    }
}

function Add-Tree($Tree, [string]$File, $State) {
    foreach ($f in $Tree.FindAll({param($n)
        $n -is [Management.Automation.Language.FunctionDefinitionAst]
    }, $true)) { $State.definitions[$f.Name] = $true }
    foreach ($c in $Tree.FindAll({param($n)
        $n -is [Management.Automation.Language.CommandAst]
    }, $true)) {
        $name = $c.GetCommandName()
        if ($name -match '^[A-Za-z][A-Za-z0-9_]*-[A-Za-z][A-Za-z0-9_-]*$') {
            $State.calls.Add(@{name=$name; file=$File; line=$c.Extent.StartLineNumber})
        }
        if ($name -eq 'Import-Module') {
            $paths=@(Resolve-StaticPath $c $Tree @{})
            foreach ($path in $paths) { Add-File $path $State }
            if (-not $paths.Count) {
                # All current OS imports are pinned to these absolute PSHOME
                # module paths. Other module imports require a readable source.
                $osModules=@($c.FindAll({param($n)
                    $n -is [Management.Automation.Language.StringConstantExpressionAst] -and
                    $n.Value -match '^Modules[\\/]Microsoft\.PowerShell\.(Utility|Security)[\\/]Microsoft\.PowerShell\.(Utility|Security)\.psd1$'
                }, $true))
                if ($osModules.Count -ne 1) {
                    $State.problems.Add(('{0}:{1}: unresolved Import-Module' -f $File,$c.Extent.StartLineNumber))
                }
            }
        }
        if ($c.InvocationOperator -ne 'Dot' -or ($name -and $name -notmatch '\.ps(m)?1$')) { continue }
        $target = $c.CommandElements[0]
        $scope = Get-Scope $c
        $variables = @($target.FindAll({param($n)
            $n -is [Management.Automation.Language.VariableExpressionAst]
        }, $true) | ForEach-Object { $_.VariablePath.UserPath })
        $strings = @($target.FindAll({param($n)
            $n -is [Management.Automation.Language.StringConstantExpressionAst]
        }, $true) | ForEach-Object { $_.Value })
        $commands = @($target.FindAll({param($n)
            $n -is [Management.Automation.Language.CommandAst]
        }, $true) | ForEach-Object { $_.GetCommandName() })
        $loads = @()
        if ($commands -contains 'Get-LabelSharedPortableLeafPath') {
            $leaf = @($strings | Where-Object { $_ -match '\.ps1$' })
            if (-not $leaf.Count) { $leaf = @('portable.ps1') }
            $loads = @($leaf | ForEach-Object { 'kmtech_shared/powershell/' + $_ })
        }
        elseif ($variables -contains 'definitions') {
            # Import-LabelOfficialLegacyTaskFunctions deliberately imports only
            # the named AST definitions, not the entire placement entrypoint.
            $source = Parse-Source 'INSTALL_THIS_PC.ps1'
            foreach ($s in $scope.FindAll({param($n)
                $n -is [Management.Automation.Language.StringConstantExpressionAst]
            }, $true)) {
                if ($s.Value -notmatch '^[A-Za-z]+-Label[A-Za-z]+$') { continue }
                $selected = @($source.FindAll({param($n)
                    $n -is [Management.Automation.Language.FunctionDefinitionAst] -and
                    $n.Name -ceq $s.Value
                }, $true))
                if ($selected.Count -ne 1) { throw 'G62 selected function missing' }
                Add-Tree $selected[0] 'INSTALL_THIS_PC.ps1' $State
            }
            continue
        }
        elseif ($variables -contains 'text') {
            # Pinned atomic helpers dot-source a literal function body. Parse
            # that body as code; do not execute it or count unrelated templates.
            $bodies = @($scope.FindAll({param($n)
                $n -is [Management.Automation.Language.AssignmentStatementAst] -and
                $n.Left.Extent.Text -ceq '$text'
            }, $true))
            $found = $false
            foreach ($b in $bodies) {
                foreach ($s in $b.Right.FindAll({param($n)
                    $n -is [Management.Automation.Language.StringConstantExpressionAst]
                }, $true)) {
                    if ($s.Value -notmatch '(?im)^\s*function\s+') { continue }
                    $t=$null; $e=$null
                    $inline=[Management.Automation.Language.Parser]::ParseInput($s.Value,[ref]$t,[ref]$e)
                    if ($e.Count) { throw 'G62 inline parse failed' }
                    Add-Tree $inline ($File + ':inline@' + $s.Extent.StartLineNumber) $State
                    $found=$true
                }
            }
            if (-not $found) { throw 'G62 inline body unresolved' }
            continue
        }
        elseif ($variables -contains 'Action' -and $scope.Name -eq 'Invoke-LabelRollbackStep') {
            # This is a caller-supplied scriptblock, already present in this AST.
            continue
        }
        else {
            # FreezePlacementHelper returns a verified copy of the placement
            # entry plus its integrity file. These aliases describe copies of
            # source bytes, not extra ambient function definitions.
            $aliases = @{
                integrityBytes='tools/bootstrap_integrity.ps1'
                preparationIntegrityBytes='tools/bootstrap_integrity.ps1'
                frozenPlacement='INSTALL_THIS_PC.ps1'
                frozen='INSTALL_THIS_PC.ps1'
            }
            foreach ($v in $variables) {
                if ($aliases.ContainsKey($v)) { $loads += $aliases[$v] }
            }
            if (-not $loads.Count) { $loads=@(Resolve-StaticPath $target $Tree @{}) }
        }
        if (-not $loads.Count) {
            $State.problems.Add(('{0}:{1}: unresolved dot-source' -f $File,$c.Extent.StartLineNumber))
        }
        foreach ($load in $loads) { Add-File $load $State }
    }
}

function Add-File([string]$File, $State) {
    if ($State.files.ContainsKey($File)) { return }
    $State.files[$File]=$true
    Add-Tree (Parse-Source $File) $File $State
}

function New-State {
    return @{definitions=@{}; files=@{};
        calls=(New-Object 'System.Collections.Generic.List[object]');
        problems=(New-Object 'System.Collections.Generic.List[string]')}
}

# A definition in a different entry must never make this entry pass. Exercise
# the same AST walker and lookup, without loading or launching fixture code.
$fixture = New-State
$t=$null; $e=$null
$tree=[Management.Automation.Language.Parser]::ParseInput(
    'function Test-G62Defined {}; Test-G62Defined; Invoke-G62Missing; $unused = ''function Invoke-G62Missing {}''', [ref]$t,[ref]$e)
Add-Tree $tree '<fixture>' $fixture
$otherEntry=New-State
$otherTree=[Management.Automation.Language.Parser]::ParseInput(
    'function Invoke-G62Missing {}', [ref]$t,[ref]$e)
Add-Tree $otherTree '<different-entry>' $otherEntry
$missing=@($fixture.calls | Where-Object { -not $fixture.definitions.ContainsKey($_.name) })
$selfCheck=($missing.Count -eq 1 -and $missing[0].name -eq 'Invoke-G62Missing')
$problems=New-Object 'System.Collections.Generic.List[string]'
$entries=New-Object 'System.Collections.Generic.List[object]'
foreach ($entry in (Get-Content -LiteralPath $EntriesPath -Raw | ConvertFrom-Json)) {
    $state=New-State
    Add-File $entry $state
    foreach ($p in $state.problems) { $problems.Add($entry + ': ' + $p) }
    foreach ($call in $state.calls) {
        if (-not $state.definitions.ContainsKey($call.name) -and
            -not $Inbox.ContainsKey($call.name) -and -not $Allowed.ContainsKey($call.name)) {
            $problems.Add(('{0}: {1}:{2}: {3}' -f $entry,$call.file,$call.line,$call.name))
        }
    }
    $entries.Add(@{entry=$entry; files=@($state.files.Keys | Sort-Object);
        definitions=$state.definitions.Count; calls=$state.calls.Count})
}
@{self_check=$selfCheck; entries=$entries.ToArray(); problems=@($problems | Sort-Object -Unique);
    allowance_count=$Allowed.Count; inbox_count=$Inbox.Count} |
    ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $ResultPath -Encoding UTF8
