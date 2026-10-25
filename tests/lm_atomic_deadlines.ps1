param([string]$HostRoot)
$ErrorActionPreference='Stop'
Add-Type -TypeDefinition @'
using System;
using System.IO;
public class AtomicDeadlineTestClock {
 public static double Now;
 private double started = Now;
 public TimeSpan Elapsed { get { return TimeSpan.FromMilliseconds(Now - started); } }
 public static AtomicDeadlineTestClock StartNew() { return new AtomicDeadlineTestClock(); }
}
public class OtherAccessRefusal : UnauthorizedAccessException {
 public OtherAccessRefusal() : base("test refusal") { HResult = unchecked((int)0x80070057); }
}
public static class AtomicDeadlineTestFile {
 public static int Calls;
 public static string Scenario;
 public static Exception First;
 static void Attempt() {
  Calls++;
  if (Calls > 1 && Scenario != "persistent") return;
  Exception error = Scenario == "other" ? (Exception)new OtherAccessRefusal() : new UnauthorizedAccessException("test refusal");
  if (First == null) First = error;
  throw error;
 }
 public static void Move(string source,string destination) { Attempt(); }
 public static void Replace(string source,string destination,string backup) { Attempt(); }
 public static bool MoveFileExW(string source,string destination,uint flags) { Calls++;return true; }
}
'@
$tokens=$null;$errors=$null
$tree=[Management.Automation.Language.Parser]::ParseFile((Join-Path $HostRoot 'INSTALL_THIS_PC.ps1'),[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'SOURCE_PARSE'}
$body=@($tree.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Invoke-LabelAtomicReplace'},$false))
if($body.Count -ne 1){throw 'SOURCE_COUNT'}
# Substitute only the clock and native file boundary. The retry/classification
# and error propagation statements are the real parsed product function.
$source=$body[0].Extent.Text.Replace('[Diagnostics.Stopwatch]::StartNew()','[AtomicDeadlineTestClock]::StartNew()').Replace('[IO.File]::Move','[AtomicDeadlineTestFile]::Move').Replace('[IO.File]::Replace','[AtomicDeadlineTestFile]::Replace').Replace('[KMTechStartupUpdateAtomicFile]::MoveFileExW','[AtomicDeadlineTestFile]::MoveFileExW')
. ([scriptblock]::Create($source))
$script:sleeps=0
function Start-Sleep {param([int]$Milliseconds)
 if($Milliseconds -le 0 -or $Milliseconds -gt 50){throw 'INTERVAL'}
 $script:sleeps++
 if([AtomicDeadlineTestFile]::Scenario -ceq 'oversleep'){[AtomicDeadlineTestClock]::Now=1100}
 else{[AtomicDeadlineTestClock]::Now+=$Milliseconds}
}
foreach($mode in @('Move','Replace')){
 foreach($scenario in @('once','persistent','oversleep','other')){
  [AtomicDeadlineTestClock]::Now=0;[AtomicDeadlineTestFile]::Calls=0
  [AtomicDeadlineTestFile]::First=$null;[AtomicDeadlineTestFile]::Scenario=$scenario
  $script:sleeps=0;$failure=$null
  try{Invoke-LabelAtomicReplace 'source' 'target' -Mode $mode}catch{$failure=$_.Exception.GetBaseException()}
  if($scenario -ceq 'once'){
   if($null -ne $failure -or [AtomicDeadlineTestFile]::Calls -ne 2 -or $script:sleeps -ne 1){throw 'ACCESS_FIVE_ONCE'}
  }else{
   if(-not [object]::ReferenceEquals($failure,[AtomicDeadlineTestFile]::First)){throw 'FIRST_ERROR_IDENTITY'}
   if($scenario -ceq 'persistent' -and ([AtomicDeadlineTestClock]::Now -ne 1000 -or [AtomicDeadlineTestFile]::Calls -ne 20)){throw 'PERSISTENT_DEADLINE'}
   if($scenario -ceq 'oversleep' -and ([AtomicDeadlineTestFile]::Calls -ne 1 -or $script:sleeps -ne 1)){throw 'POST_DEADLINE_NATIVE_CALL'}
   if($scenario -ceq 'other' -and ([AtomicDeadlineTestFile]::Calls -ne 1 -or $script:sleeps -ne 0)){throw 'OTHER_ACCESS_RETRIED'}
  }
 }
}
# One-time type preparation precedes the native publication deadline.
$script:declarations=0
function Add-Type {param([string]$TypeDefinition)
 $script:declarations++;[AtomicDeadlineTestClock]::Now=1100
}
$source=$source.Replace("'KMTechStartupUpdateAtomicFile' -as [type]","'MissingAtomicDeadlineDeclaration' -as [type]")
. ([scriptblock]::Create($source))
[AtomicDeadlineTestClock]::Now=0;[AtomicDeadlineTestFile]::Calls=0;$failure=$null
try{Invoke-LabelAtomicReplace 'source' 'target' -Mode WriteThrough}catch{$failure=$_.Exception.GetBaseException()}
if($null -ne $failure -or $script:declarations -ne 1 -or [AtomicDeadlineTestFile]::Calls -ne 1){throw 'COLD_DECLARATION_CONSUMED_DEADLINE'}
'PASS'
