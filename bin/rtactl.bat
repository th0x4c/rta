@echo off
setlocal enabledelayedexpansion

set RTA_HOME=%~dp0..\

set JRUBY_CMD=
for %%i in (jruby.bat) do if exist %%~$PATH:i set JRUBY_CMD=%%~$PATH:i
if "!JRUBY_CMD!" == "" (
  set JAVA_MEM=-Xmx500m
  set JAVA_STACK=-Xss1024k
  set JAVA_VM=-client
  set JAVA_OPTS=!JAVA_VM! !JAVA_MEM! !JAVA_STACK!
  set JRUBY_CMD=java !JAVA_OPTS! org.jruby.Main
)

rem RUBYOPT Ç™éwíËÇ≥ÇÍÇƒÇ¢ÇÈÇ∆ÉGÉâÅ[Ç…Ç»ÇÈÇΩÇﬂ RUBYOPT Ç unset Ç∑ÇÈ
set RUBYOPT=

!JRUBY_CMD! !RTA_HOME!\bin\rtactl.rb %*
