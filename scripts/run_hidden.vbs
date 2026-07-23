Option Explicit

Dim shell
Dim target
Dim exitCode

If WScript.Arguments.Count < 1 Then
    WScript.Echo "Usage: wscript.exe run_hidden.vbs <cmd-file>"
    WScript.Quit 64
End If

target = WScript.Arguments(0)
Set shell = CreateObject("WScript.Shell")
exitCode = shell.Run("""" & target & """", 0, True)
WScript.Quit exitCode
