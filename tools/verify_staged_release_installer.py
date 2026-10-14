#!/usr/bin/env python
"""Verify the staged Label_Match code-only bootstrap package."""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
from typing import Any, Mapping, Sequence


REPORT_SCHEMA = "label-match-staged-installer-verification-v3"
OUTPUT_BOUND_BYTES = 64 * 1024
TIMEOUT_SECONDS = 120
REQUIRED_MEMBERS = {
    "INSTALL_THIS_PC.ps1",
    "Label_Match.exe",
    "contract.lock.json",
    "_internal/config/app_settings.json",
    "tools/bootstrap_integrity.ps1",
    "tools/failure_redaction.ps1",
    "tools/failure_redaction_rules.json",
    "kmtech_shared.lock.json",
    "kmtech_shared.manifest.json",
    "kmtech_shared/powershell/portable.ps1",
    "kmtech_shared/powershell/data_generation.ps1",
    "kmtech_shared/powershell/ps51_module_prologue.ps1",
}
FORBIDDEN_ACTIVE_AUTHORITY_MEMBERS = {
    "install_label_match_direct_sync.ps1",
    "tools/direct_sync_relay_install_pack.py",
    "tools/direct_sync_relay_install_pack.exe",
    "tools/direct_sync_relay_install_pack/direct_sync_relay_install_pack.exe",
    "tools/direct_sync_relay_runner/direct_sync_relay_runner.exe",
    "tools/register_label_match_worker_pc.exe",
    "tools/invoke_embedded_python.ps1",
}


# Supported statically named bootstrap operations. Everything else is rejected,
# including code factories/evaluators, jobs, reflection/assembly loaders and
# dynamic member names. These are fixed stock inputs, never candidate-derived.
BOOTSTRAP_STATIC_SOURCE = {
    'module_prologue': [
        r"Import-Module -Name (Join-Path $PSHOME 'Modules\Microsoft.PowerShell.Utility\Microsoft.PowerShell.Utility.psd1') -ErrorAction Stop",
        r"Import-Module -Name (Join-Path $PSHOME 'Modules\Microsoft.PowerShell.Security\Microsoft.PowerShell.Security.psd1') -ErrorAction Stop",
    ],
    'casts': """
        bool byte[] char char[] int int64 object[] ordered pscustomobject ref
        security.accesscontrol.aceflags security.accesscontrol.filesystemrights string
        system.comparison[object] uint64 void xml
    """.split(),
    'commands': """
        assert-alreadyelevated assert-bootstrapintegrityrecord assert-hardenedcodeacl
        assert-kmtechmachinemetadatatext assert-labelofficiallegacytaskstopped
        assert-labelprotectedinstallpath assert-labelremovalboundary
        assert-labelremovalquiescent assert-labelstartupupdatesettings
        assert-labelstartupupdatetaskdefinition assert-labelstartupupdatetrust
        assert-noreparsepoint assert-ordinarylegacylauncher assert-placementdatadowngrade
        assert-protectedcodeancestors assert-requiredrelease convertfrom-json convertto-json
        convertto-kmtechpublicrecord convertto-kmtechredactedsinkobject
        convertto-kmtechredactedsinktext convertto-labellegacytaskpath convertto-labeltasksid
        convertto-normalizedaclrights copy-item disable-scheduledtask enable-scheduledtask
        enter-labelcanonicalplacement enter-labelwriterdelegatedoperation
        exit-labelwriteradmission export-scheduledtask foreach-object get-acl
        get-authenticodesignature get-bootstrapcodeinventory get-bootstrapfilesha256
        get-bootstrapinventoryaggregate get-bootstraprelativecodepath
        get-bootstrapstrictfullpath get-childitem get-ciminstance get-codeinventory get-command
        get-content get-date get-earlyfilesha256 get-filesha256
        get-installationpreparationexitcode get-inventoryaggregate get-item get-itemproperty
        get-kmtechancestoraclshapekey get-kmtechbootstrapinventoryaggregate
        get-kmtechbootstraprelativecodepath get-kmtechbootstrapstrictfullpath
        get-kmtechdatadowngradedecision get-kmtechsharedancestoraclstate
        get-labelartifactfileplan get-labelofficiallegacyprincipalmodes
        get-labelofficiallegacytaskexpectation get-labelofficiallegacytaskexpectations
        get-labelofficiallegacytaskfingerprint get-labelofficiallegacytasksnapshot
        get-labelremovalartifacts get-labelremovaliopath get-labelsharedportableleafpath
        get-labelstartupupdatetask get-labelstartupupdatetaskpreimage
        get-labelstartupupdatetaskspec get-labelsupportedstartuptasks
        get-legacytaskbynamefailclosed get-ownedlegacytaskretirementsnapshot
        get-placementdataversion get-requiredexternalinteger get-scheduledtask
        get-strictfullpath initialize-labelprotectedinstallbase initialize-labelstartupupdate
        install-currentusertlscabootstrap invoke-labelconvergentremoval invoke-labelatomicreplace
        invoke-labelofficiallegacytasktransition invoke-labelstartupupdatetaskmutation join-path
        measure-object move-item new-item new-object open-labelrunkey out-null
        protect-kmtechcodeancestors protect-labelinstallerownerfile read-labelwriterfence
        register-scheduledtask remove-item remove-labelemptiedparents remove-labelscheduledtasks
        remove-ownedlegacytask restore-labelstartupupdatepreimage
        save-labelstartupupdatepreimage set-acl set-hardenedcodeacl sort-bootstrapinventory
        sort-object split-path start-sleep test-currentuserrelaypersistencepresent
        test-kmtechprivilegedownersid test-labelremovalfile test-path test-samepath
        unregister-scheduledtask where-object write-bootstrapintegrityrecord
        write-bootstraputf8json write-elevationlog write-kmtechprotectedfailuresidecar
        write-labelancestorpreparationevent write-labelinstallerownerrecord
        write-labelstartupupdaterecord write-output write-warning
    """.split(),
    'instance_methods': """
        add addaccessrule addnamespace addseconds clonenode compare computehash connect contains
        containskey createshortcut deletetask dispose endswith equals flush get_innertext get_innerxml
        getaccessrules getbaseexception getbinaryform getbytes getfolder getgroup getinstances
        getowner getsecuritydescriptor getsecuritydescriptorbinaryform
        getsecuritydescriptorsddlform getstring gettask gettype getvalue getvaluenames indexof
        isinrole loadxml readtoend remove removeaccessrulespecific removechild replace
        selectnodes selectsinglenode set_innertext setaccessruleprotection setsecuritydescriptor
        setsecuritydescriptorbinaryform split startswith stop substring toarray tofiletimeutc
        tolowerinvariant tostring touniversaltime translate trim trimend trimstart write
        writeline
    """.split(),
    'static_methods': """
        [array]::sort [bitconverter]::tostring [byte[]]::new
        [collections.generic.list[object]]::new [componentmodel.win32exception]::new
        [diagnostics.process]::getprocessbyid [diagnostics.stopwatch]::startnew [environment]::expandenvironmentvariables
        [environment]::getfolderpath
        [guid]::newguid [intptr]::new [invalidoperationexception]::new
        [io.directory]::createdirectory [io.file]::appendalltext [io.file]::getattributes
        [io.file]::open [io.file]::openread [io.file]::readallbytes [io.file]::readalltext
        [io.file]::writealltext [io.file]::replace [io.file]::move [io.path]::changeextension [io.path]::getdirectoryname
        [io.path]::getextension [io.path]::getfullpath [io.path]::getpathroot
        [io.path]::ispathrooted [io.streamreader]::new
        [kmtechlabelrunregistrynative]::regopenkeyexw
        [kmtechstartupupdateatomicfile]::movefileexw [microsoft.win32.registrykey]::fromhandle
        [microsoft.win32.safehandles.saferegistryhandle]::new [math]::min [math]::ceiling
        [runtime.interopservices.marshal]::getlastwin32error [regex]::match [regex]::matches
        [regex]::replace [security.accesscontrol.filesystemaccessrule]::new
        [security.accesscontrol.rawsecuritydescriptor]::new
        [security.cryptography.sha256]::create [security.principal.ntaccount]::new
        [security.principal.securityidentifier]::new
        [security.principal.windowsidentity]::getcurrent [string]::isnullorempty
        [string]::isnullorwhitespace [system.collections.generic.comparer[object]]::create
        [text.utf8encoding]::new [uri]::trycreate [uri]::unescapedatastring
        [xml.xmlconvert]::tostring [xml.xmlconvert]::totimespan [xml.xmlnamespacemanager]::new
        [xml]::new
    """.split(),
    'object_types': """
        io.filestream security.principal.windowsprincipal text.utf8encoding
        xml.xmlnamespacemanager schedule.service wscript.shell
    """.split(),
}


# Existing indirect calls and their variable bindings, keyed independently of lines.
BOOTSTRAP_INDIRECT_SOURCE = {
    'calls': {
        ('installer', 'Invoke-LabelAtomicReplace', 'Add-Type -TypeDefinition @\'\nusing System.Runtime.InteropServices;\npublic static class KMTechStartupUpdateAtomicFile {\n [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)]\n public static extern bool MoveFileExW(string source, string target, uint flags);\n}\n\'@'): 1,
        ('installer', 'Open-LabelRunKey', 'Add-Type -TypeDefinition @\'\nusing System;\nusing System.Runtime.InteropServices;\npublic static class KmtechLabelRunRegistryNative {\n    [DllImport("advapi32.dll", CharSet=CharSet.Unicode, ExactSpelling=true)]\n    public static extern int RegOpenKeyExW(IntPtr root, string subKey, uint options, uint access, out IntPtr key);\n}\n\'@'): 1,
        ('installer', '', '. $BootstrapIntegrityFunctions -SharedCodeRoot $PSScriptRoot'): 1,
        ('installer', '', ". (Get-LabelSharedPortableLeafPath $PSScriptRoot 'data_generation.ps1')"): 1,
        ('installer', '', ". (Join-Path $PSScriptRoot 'tools/failure_redaction.ps1')"): 1,
        ('installer', 'Set-HardenedCodeAcl', '& $icacls @ownerArgs'): 1,
        ('installer', 'Set-HardenedCodeAcl', '& $icacls @resetArgs'): 1,
        ('installer', 'Set-HardenedCodeAcl', "& $icacls $Path `\n            '/inheritance:r' `\n            '/grant:r' `\n            '*S-1-5-18:(OI)(CI)F' `\n            '*S-1-5-32-544:(OI)(CI)F' `\n            '*S-1-5-32-545:(OI)(CI)RX' `\n            '/L'"): 1,
        ('installer', 'Protect-LabelInstallerOwnerFile', "& (Join-Path ([Environment]::SystemDirectory) 'icacls.exe') $Path '/setowner' '*S-1-5-32-544' '/L'"): 1,
        ('installer', 'Get-LabelStartupUpdateTaskSpec', '& $runtime -I -B $implementation --print-logon-task --install-root $Root --operator-sid $Sid'): 1,
        ('installer', 'Invoke-LabelStartupUpdateTaskMutation', '& $Action'): 1,
        ('installer', '', "& (Join-Path $sourceRootFull 'runtime\\python.exe') -I -B `\n        (Join-Path $sourceRootFull 'app\\main.py') --label-match-lifecycle-context `\n        --install-root $applicationParent --operation install"): 1,
        ('installer', '', '& $robocopy $sourceRootFull $stagingRoot /E /XJ /MT:16 /R:0 /W:0 /NFL /NDL /NJH /NJS /NP /XF (Join-Path $sourceRootFull $IntegrityFileName)'): 1,
        ('installer', '', "& (Join-Path $sourceRootFull 'runtime\\python.exe') -I -B `\n            (Join-Path $sourceRootFull 'app\\main.py') --label-match-lifecycle-context `\n            --install-root $applicationParent --expected-from-commit $ExpectedActiveCommit `\n            --activate-supported-commit $nextCommit"): 1,
        ('helper', 'Write-BootstrapUtf8Json', ". (Join-Path $PSScriptRoot 'failure_redaction.ps1')"): 1,
        ('helper', '', '. (Get-LabelSharedPortableLeafPath $SharedCodeRoot)'): 1,
    },
    'bindings': {
        ('installer', '', "$BootstrapIntegrityFunctions = ''"): 1,
        ('installer', '', '$BootstrapIntegrityFunctions = Join-Path $PSScriptRoot "tools\\bootstrap_integrity.ps1"'): 1,
        ('installer', 'Set-HardenedCodeAcl', "$icacls = Join-Path ([Environment]::SystemDirectory) 'icacls.exe'"): 1,
        ('installer', 'Get-LabelStartupUpdateTaskSpec', "$runtime = Join-Path $Root 'runtime\\python.exe'"): 1,
        ('installer', 'Get-LabelStartupUpdateTaskSpec', "$implementation = Join-Path $Root 'app\\auto_update.py'"): 1,
        ('installer', 'Get-LabelOfficialLegacyTaskFingerprint', '$action = $actions[0]'): 1,
        ('installer', '', "$robocopy = Join-Path ([Environment]::SystemDirectory) 'robocopy.exe'"): 1,
    },
}


class StagedInstallerVerificationError(RuntimeError):
    """Raised when the staged package violates the supported topology."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(value: object) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _files(root: Path) -> list[Path]:
    return sorted(
        (path for path in root.rglob("*") if path.is_file()),
        key=lambda path: (
            path.relative_to(root).as_posix().casefold(),
            path.relative_to(root).as_posix(),
        ),
    )


def _inventory(root: Path) -> list[dict[str, object]]:
    return [
        {
            "path": path.relative_to(root).as_posix(),
            "size": path.stat().st_size,
            "sha256": _sha256(path),
        }
        for path in _files(root)
        if path.name not in {"staged-installer-verification.json", "build-manifest.json"}
    ]


def _inventory_digest(inventory: list[dict[str, object]]) -> str:
    payload = json.dumps(
        inventory,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _assert_safe_package(package_root: Path) -> set[str]:
    if not package_root.is_dir():
        raise StagedInstallerVerificationError("package root is absent")
    paths: set[str] = set()
    folded: set[str] = set()
    root = package_root.resolve()
    for candidate in package_root.rglob("*"):
        if candidate.is_symlink():
            raise StagedInstallerVerificationError(
                f"package reparse/symlink entry is forbidden: {candidate}"
            )
        if not candidate.is_file():
            continue
        relative = candidate.resolve().relative_to(root).as_posix()
        pure = PurePosixPath(relative)
        if pure.is_absolute() or any(part in {"", ".", ".."} for part in pure.parts):
            raise StagedInstallerVerificationError("package member path is unsafe")
        key = relative.casefold()
        if key in folded:
            raise StagedInstallerVerificationError("case-colliding package members exist")
        folded.add(key)
        paths.add(relative)
    missing = sorted(REQUIRED_MEMBERS - paths)
    if missing:
        raise StagedInstallerVerificationError(
            f"required staged members are absent: {missing}"
        )
    forbidden = sorted(FORBIDDEN_ACTIVE_AUTHORITY_MEMBERS & paths)
    if forbidden:
        raise StagedInstallerVerificationError(
            f"retired active task-authority members remain packaged: {forbidden}"
        )
    if not (package_root / "_internal").is_dir():
        raise StagedInstallerVerificationError("onedir _internal runtime is absent")
    return paths


def _assert_bootstrap_source(installer: Path, integrity_helper: Path) -> None:
    source = installer.read_text(encoding="utf-8-sig")
    helper_source = integrity_helper.read_text(encoding="utf-8-sig")
    lowered = source.casefold()
    helper_lowered = helper_source.casefold()
    installer_required = (
        "assert-alreadyelevated",
        "production placement requires integrity functions preloaded",
        "canonical pinned in-memory launcher",
        "bootstrap-integrity.json",
        "tools\\bootstrap_integrity.ps1",
        "write-bootstrapintegrityrecord",
        "identity_profile_created=false",
        "elevation_points=1:code_placement",
        "kmtech.labelmatch.relay",
        "remove-ownedlegacytask",
        "register-scheduledtask",
    )
    helper_required = (
        "label-match-bootstrap-integrity-v1",
        "get-bootstrapcodeinventory",
        "state_scope = 'current_user_first_run'",
        "identity_profile_created = $false",
    )
    missing = [token for token in installer_required if token not in lowered]
    missing.extend(
        f"integrity-helper:{token}"
        for token in helper_required
        if token not in helper_lowered
    )
    if missing:
        raise StagedInstallerVerificationError(
            f"code-only bootstrap contract is incomplete: {missing}"
        )
    forbidden = (
        "new-scheduledtasktrigger",
        "new-scheduledtaskprincipal",
        "schtasks.exe",
        "install_label_match_direct_sync.ps1",
        "--source-host-id",
        "sourcehostid",
        "-verb runas",
    )
    combined_lowered = lowered + "\n" + helper_lowered
    present = [token for token in forbidden if token in combined_lowered]
    if "register-scheduledtask" in combined_lowered:
        # Placement owns two authenticated current-user startup task mutations.
        # Parse their actual command/ancestor structure; a registration added
        # elsewhere (including one with the same spelling) remains forbidden.
        environment = dict(os.environ)
        environment["KMTECH_STAGED_INSTALLER_SOURCE"] = str(installer.resolve())
        environment["KMTECH_STAGED_SOURCE_POLICY"] = json.dumps(BOOTSTRAP_STATIC_SOURCE)
        environment["KMTECH_STAGED_INTEGRITY_SOURCE"] = str(integrity_helper.resolve())
        parsed = subprocess.run(
            ["powershell.exe", "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", r"""
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile($env:KMTECH_STAGED_INSTALLER_SOURCE,[ref]$tokens,[ref]$errors)
if($errors.Count){exit 1}
$helper=[Management.Automation.Language.Parser]::ParseFile($env:KMTECH_STAGED_INTEGRITY_SOURCE,[ref]$tokens,[ref]$errors)
if($errors.Count){exit 1}
# Inspect commands, not definition strings or comments. Normalize module/path
# qualification before classifying task registration and execution aliases.
$policy=ConvertFrom-Json $env:KMTECH_STAGED_SOURCE_POLICY
$calls=@();$bindings=@();$rows=@();$delegations=@()
foreach($entry in @(@('installer',$ast),@('helper',$helper))){
 $location=$entry[0];$tree=$entry[1]
 foreach($node in $tree.FindAll({param($n) $n -is [Management.Automation.Language.CommandAst]},$true)){
  $owner='';$mutation=$false
  for($parent=$node.Parent;$null -ne $parent;$parent=$parent.Parent){
   if($parent -is [Management.Automation.Language.CommandAst] -and $parent.GetCommandName() -ceq 'Invoke-LabelStartupUpdateTaskMutation'){$mutation=$true}
   if($parent -is [Management.Automation.Language.FunctionDefinitionAst]){$owner=$parent.Name;break}
  }
  $row=[pscustomobject]@{location=$location;owner=$owner;command=$node.Extent.Text.Replace("`r`n","`n")}
  $name=$node.GetCommandName()
  if([string]::IsNullOrEmpty($name)){$calls+=$row;continue}
  $name=($name -split '[\\/]')[-1]
  # The only code-generation exceptions are the two pinned Win32 declarations.
  if($name -ieq 'Add-Type'){$calls+=$row;continue}
  if($name -ieq 'Import-Module'){
   if($row.command -cnotin $policy.module_prologue){exit 1}
   continue
  }
  if($name -notin $policy.commands){exit 1}
  # These pipeline commands otherwise invoke nonliteral ScriptBlocks or
  # a named member (ForEach-Object -MemberName Invoke / % Invoke).
  if($name -iin @('ForEach-Object','Where-Object')){
   foreach($argument in @($node.CommandElements | Select-Object -Skip 1)){
    if($argument -is [Management.Automation.Language.ScriptBlockExpressionAst]){continue}
    if($argument -is [Management.Automation.Language.CommandParameterAst] -and
       $argument.ParameterName -iin @('Begin','Process','End','FilterScript') -and
       ($null -eq $argument.Argument -or $argument.Argument -is [Management.Automation.Language.ScriptBlockExpressionAst])){continue}
    exit 1
   }
  }
  if($name -iin @('Sort-Object','Measure-Object')){
   foreach($argument in @($node.CommandElements | Select-Object -Skip 1)){
    if($argument -is [Management.Automation.Language.StringConstantExpressionAst] -or
       $argument -is [Management.Automation.Language.ScriptBlockExpressionAst]){continue}
    if($argument -is [Management.Automation.Language.CommandParameterAst] -and
       ($null -eq $argument.Argument -or
        $argument.Argument -is [Management.Automation.Language.StringConstantExpressionAst] -or
        $argument.Argument -is [Management.Automation.Language.ScriptBlockExpressionAst])){continue}
    exit 1
   }
  }
  if($name -ieq 'New-Item'){
   $itemTypes=@($node.CommandElements | Where-Object {
     $_ -is [Management.Automation.Language.CommandParameterAst] -and $_.ParameterName -ieq 'ItemType'})
   if($itemTypes.Count -ne 1){exit 1}
   $type=$itemTypes[0].Argument
   if($null -eq $type){$type=$node.CommandElements[$node.CommandElements.IndexOf($itemTypes[0])+1]}
   if($type -isnot [Management.Automation.Language.StringConstantExpressionAst] -or
      $type.Value -notin @('File','Directory')){exit 1}
  }
  if($name -ieq 'New-Object'){
   $type=$node.CommandElements[1]
   if($type -is [Management.Automation.Language.CommandParameterAst]){
    if($type.ParameterName -notin @('TypeName','ComObject')){exit 1}
    if($null -ne $type.Argument){$type=$type.Argument}else{$type=$node.CommandElements[2]}
   }
   if($type -isnot [Management.Automation.Language.StringConstantExpressionAst] -or
      $type.Value -notin $policy.object_types){exit 1}
  }
  if($name -ieq 'Register-ScheduledTask'){
   if($location -eq 'helper'){exit 1}
   $rows+=[pscustomobject]@{owner=$owner;line=$node.Extent.StartLineNumber;mutation=$mutation;command=$row.command}
  }
  if($name -ieq 'Invoke-LabelStartupUpdateTaskMutation'){
   if($location -ne 'installer' -or $node.CommandElements.Count -ne 2 -or
      $node.CommandElements[1] -isnot [Management.Automation.Language.ScriptBlockExpressionAst]){exit 1}
   $delegations+=$owner
  }
 }
 # Default-deny member invocation, rather than a list of banned spellings.
 # Constructors and factories also require their exact static receiver type.
 foreach($node in $tree.FindAll({param($n) $n -is [Management.Automation.Language.InvokeMemberExpressionAst]},$true)){
  if($node.Member -isnot [Management.Automation.Language.StringConstantExpressionAst]){exit 1}
  $member=$node.Member.Value
  if($node.Static){
   if($node.Expression -isnot [Management.Automation.Language.TypeExpressionAst] -or
      ($node.Expression.Extent.Text+'::'+$member) -notin $policy.static_methods){exit 1}
   if($node.Expression.TypeName.FullName -ieq 'System.Collections.Generic.Comparer[object]' -and
      $member -ieq 'Create'){
    if($node.Arguments.Count -ne 1 -or
       $node.Arguments[0] -isnot [Management.Automation.Language.ConvertExpressionAst] -or
       $node.Arguments[0].Child -isnot [Management.Automation.Language.ScriptBlockExpressionAst]){exit 1}
   }
   if($node.Expression.TypeName.FullName -ieq 'Array' -and $member -ieq 'Sort'){
    if($node.Arguments.Count -ne 2 -or
       $node.Arguments[1] -isnot [Management.Automation.Language.InvokeMemberExpressionAst] -or
       $node.Arguments[1].Expression.Extent.Text -ine '[System.Collections.Generic.Comparer[object]]' -or
       $node.Arguments[1].Member.Value -ine 'Create'){exit 1}
   }
   if($node.Expression.TypeName.FullName -ieq 'regex' -and $member -ieq 'Replace'){
    if($node.Arguments.Count -ne 3 -or
       $node.Arguments[2] -isnot [Management.Automation.Language.StringConstantExpressionAst]){exit 1}
   }
  }elseif($member -notin $policy.instance_methods){exit 1}
  elseif($member -ieq 'Replace'){
   if($node.Arguments.Count -ne 2 -or @($node.Arguments | Where-Object {
       $_ -isnot [Management.Automation.Language.StringConstantExpressionAst]}).Count){exit 1}
  }
 }
 # A cast/-as to ScriptBlock can compile strings without calling Create().
 foreach($node in $tree.FindAll({param($n) $n -is [Management.Automation.Language.ConvertExpressionAst]},$true)){
  if($node.Type.TypeName.FullName -notin $policy.casts){exit 1}
  if($node.Type.TypeName.FullName -ieq 'System.Comparison[object]' -and
     $node.Child -isnot [Management.Automation.Language.ScriptBlockExpressionAst]){exit 1}
 }
 foreach($node in $tree.FindAll({param($n) $n -is [Management.Automation.Language.BinaryExpressionAst]},$true)){
  if($node.Operator -eq 'As' -and
     ($node.Right -isnot [Management.Automation.Language.TypeExpressionAst] -or
      $node.Right.TypeName.FullName -ine 'type')){exit 1}
 }
 foreach($node in $tree.FindAll({param($n) $n -is [Management.Automation.Language.AssignmentStatementAst]},$true)){
  $variables=@($node.Left.FindAll({param($n) $n -is [Management.Automation.Language.VariableExpressionAst]},$true))
  if(-not @($variables | Where-Object {($_.VariablePath.UserPath -split ':')[-1] -iin @('BootstrapIntegrityFunctions','icacls','runtime','implementation','robocopy','Action')}).Count){continue}
  $owner=''
  for($parent=$node.Parent;$null -ne $parent;$parent=$parent.Parent){
   if($parent -is [Management.Automation.Language.FunctionDefinitionAst]){$owner=$parent.Name;break}
  }
  $bindings+=[pscustomobject]@{location=$location;owner=$owner;command=$node.Extent.Text.Replace("`r`n","`n")}
 }
}
$mutationFunctions=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Invoke-LabelStartupUpdateTaskMutation'},$true))
if($mutationFunctions.Count -ne 1 -or $mutationFunctions[0].Parameters.Count -ne 1 -or
   $mutationFunctions[0].Parameters[0].Extent.Text -cne '[scriptblock]$Action'){exit 1}
ConvertTo-Json -InputObject @{registrations=$rows;calls=$calls;bindings=$bindings;delegations=$delegations} -Depth 5 -Compress
"""],
            env=environment, capture_output=True, text=True,
            timeout=TIMEOUT_SECONDS, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        # Bind exact registration commands to their mutation owners. Harmless
        # source layout edits may move their line numbers between releases.
        allowed = {
            "Restore-LabelStartupUpdatePreimage": "Register-ScheduledTask -TaskName $name -Xml ([string]$saved.task.xml) -Force",
            "Initialize-LabelStartupUpdate": "Register-ScheduledTask -TaskName $spec.name -Xml $spec.xml -Force",
        }
        try:
            parsed_source = json.loads(parsed.stdout)
            registrations = parsed_source["registrations"]
            indirect_valid = all(
                Counter((row["location"], row["owner"], row["command"])
                        for row in parsed_source[group]) == expected
                for group, expected in BOOTSTRAP_INDIRECT_SOURCE.items()
            )
            valid = (parsed.returncode == 0 and not parsed.stderr
                     and indirect_valid
                     and Counter(parsed_source["delegations"]) == {
                         "Restore-LabelStartupUpdatePreimage": 1,
                         "Initialize-LabelStartupUpdate": 1,
                     }
                     and len(parsed.stdout.encode()) <= OUTPUT_BOUND_BYTES
                     and isinstance(registrations, list)
                     and len(registrations) == len(allowed)
                     and {row["owner"] for row in registrations} == set(allowed)
                     and all(row["mutation"] is True
                             and row["command"] == allowed[row["owner"]]
                             for row in registrations))
        except (ValueError, TypeError, KeyError):
            valid = False
        if not valid:
            present.append("register-scheduledtask")
    if present:
        failure = StagedInstallerVerificationError(
            f"bootstrap exposes retired enrollment/task authority: {present}"
        )
        failure.legacy_authority_contract = {"task_creation_tokens_absent": False}
        raise failure


def _run_bootstrap_dry_run(package_root: Path) -> tuple[bytes, bytes]:
    powershell = (
        Path(os.environ.get("SystemRoot", r"C:\Windows"))
        / "System32"
        / "WindowsPowerShell"
        / "v1.0"
        / "powershell.exe"
    )
    if not powershell.is_file():
        raise StagedInstallerVerificationError("Windows PowerShell is unavailable")
    install_root = package_root.parent / ".label-match-staged-bootstrap-dry-run" / "current"
    env = dict(os.environ)
    env["KMTECH_FACTORY_INSTALL_TEST_MODE"] = "1"
    completed = subprocess.run(
        [
            str(powershell),
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(package_root / "INSTALL_THIS_PC.ps1"),
            "-DryRun",
            "-SourceRoot",
            str(package_root),
            "-InstallRoot",
            str(install_root),
            "-AllowNoncanonicalLayoutForTest",
        ],
        check=False,
        capture_output=True,
        timeout=TIMEOUT_SECONDS,
        env=env,
    )
    stdout = completed.stdout[: OUTPUT_BOUND_BYTES + 1]
    stderr = completed.stderr[: OUTPUT_BOUND_BYTES + 1]
    if len(stdout) > OUTPUT_BOUND_BYTES or len(stderr) > OUTPUT_BOUND_BYTES:
        raise StagedInstallerVerificationError("bootstrap output exceeded its bound")
    if completed.returncode != 0:
        raise StagedInstallerVerificationError(
            f"bootstrap dry run failed with exit code {completed.returncode}"
        )
    decoded = stdout.decode("utf-8", errors="replace")
    for token in (
        "bootstrap_status=DRY_RUN",
        "identity_profile_created=false",
        "elevation_points=1:code_placement",
    ):
        if token not in decoded:
            raise StagedInstallerVerificationError(
                f"bootstrap dry-run output omitted {token}"
            )
    if stderr:
        raise StagedInstallerVerificationError("bootstrap dry run wrote stderr")
    if install_root.exists():
        raise StagedInstallerVerificationError("bootstrap dry run changed install state")
    return stdout, stderr


def verify_staged_package(package_root: Path) -> dict[str, Any]:
    package_root = package_root.resolve()
    paths_before = _assert_safe_package(package_root)
    installer = package_root / "INSTALL_THIS_PC.ps1"
    integrity_helper = package_root / "tools" / "bootstrap_integrity.ps1"
    _assert_bootstrap_source(installer, integrity_helper)
    inventory = _inventory(package_root)
    stdout, stderr = _run_bootstrap_dry_run(package_root)
    paths_after = _assert_safe_package(package_root)
    if paths_after != paths_before or _inventory(package_root) != inventory:
        raise StagedInstallerVerificationError("bootstrap dry run changed package bytes")
    ordered_inventory = sorted(inventory, key=lambda item: str(item["path"]))
    preseal_manifest = {
        "build_manifest_schema_version": 1,
        "payload_inventory": ordered_inventory,
        "payload_inventory_sha256": _canonical_sha256(ordered_inventory),
    }
    preseal_bytes = (
        json.dumps(preseal_manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    return {
        "schema_version": REPORT_SCHEMA,
        "status": "PASS",
        "proof_classification": "STATIC_ISOLATED_DRY_RUN",
        "dynamic_qualification": "NOT_TESTED",
        "public_entrypoint": {
            "path": "INSTALL_THIS_PC.ps1",
            "sha256": _sha256(installer),
            "integrity_helper_path": "tools/bootstrap_integrity.ps1",
            "integrity_helper_sha256": _sha256(integrity_helper),
        },
        "runtime_host": {
            "path": "Label_Match.exe",
            "sha256": _sha256(package_root / "Label_Match.exe"),
            "package_layout": "onedir",
            "relay_execution_boundary": "product_host",
            "current_user_relay_mode": "--label-match-user-relay",
            "direct_sync_relay_mode": "--label-match-direct-sync-relay",
        },
        "bootstrap_contract": {
            "canonical_code_root": r"C:\KMTech\Apps\Label_Match\current",
            "elevation_points": ["code_placement"],
            "elevation_authority": "canonical_pinned_in_memory_caller",
            "identity_profile_created": False,
            "state_scope": "current_user_first_run",
            "exact_inventory_readback": True,
            "onedir_required": True,
        },
        "state_contract": {
            "identity_scope": "current_user_per_pc",
            "profile_scope": "current_user",
            "credential_scope": "current_user_dpapi",
            "ledger_scope": "current_user",
            "operation_lease_store": "AUTHORITATIVE_SNAPSHOT_PRESERVED",
            "relay_persistence": "HKCU_RUN",
            "relay_port_contract": 18456,
            "source_host_override_required": False,
        },
        "legacy_authority_contract": {
            "system_scheduled_task_supported": False,
            # This existing archive-consumer field is scoped to retired
            # enrollment/task authority; the two exact startup sites above
            # are authenticated separately and do not grant that authority.
            "task_creation_tokens_absent": True,
            "current_user_startup_task_mutations": 2,
            "legacy_owned_task_cleanup_only": True,
            "forbidden_package_members_absent": True,
            "forbidden_members": sorted(FORBIDDEN_ACTIVE_AUTHORITY_MEMBERS),
        },
        "manifest_contract": {
            "path": "build-manifest.json",
            "sha256": hashlib.sha256(preseal_bytes).hexdigest(),
            "payload_file_count": len(inventory),
            "payload_inventory_sha256": preseal_manifest[
                "payload_inventory_sha256"
            ],
            "preseal_isolated_manifest": True,
        },
        "original_package_file_count": len(inventory),
        "original_package_inventory": inventory,
        "original_package_inventory_sha256": _inventory_digest(inventory),
        "original_package_unchanged": True,
        "system_python_required": False,
        "output_bound_bytes": OUTPUT_BOUND_BYTES,
        "timeout_seconds": TIMEOUT_SECONDS,
        "stdout_bytes": len(stdout),
        "stderr_bytes": len(stderr),
    }


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(dict(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package-root", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        report = verify_staged_package(args.package_root)
        _write_json(args.report, report)
    except Exception as exc:
        print(f"staged_installer_status=FAIL error_type={exc.__class__.__name__}")
        return 1
    print("staged_installer_status=PASS")
    print(f"staged_installer_report={args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
