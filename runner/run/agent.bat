
@echo off

REM -----------------------------------------------------------------
REM 
REM ------------------------------------------------------------------

@if not "%ECHO%" == ""  echo %ECHO%
@if "%OS%" == "Windows_NT" setlocal

REM : CMD /V:ON open delayed expansion for list files
@setlocal enabledelayedexpansion


if "%OS%" == "Windows_NT" (
  set "DIRNAME=%~dp0%"
) else (
  set DIRNAME=.\
)

if "x%JAVA_HOME%" == "x" (
   goto setup_java_home
) else (
	goto set_classpath
)


:setup_java_home
echo Please Setup JAVA_HOME
goto :mainEnd


:set_classpath
set CLASSPATH=%CLASSPATH%;%DIRNAME%lib

set LIST=
for %%i in (%DIRNAME%lib\*) do (
	set LIST=!LIST!;%%i
)

set CLASSPATH=%CLASSPATH%;%LIST%;
goto START

:START

set exec_info="%JAVA_HOME%\bin\java" -classpath %CLASSPATH%; Commander %*
echo %exec_info%
%exec_info%


:mainEnd
@endlocal
pause

