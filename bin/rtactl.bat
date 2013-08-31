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

rem 引数をカンマ区切りの文字列 ARGV にする
set ARGV='%1'
for %%a in (%*) do if !ARGV! == '%1' (
    set arg='%%a' 
    set ARGV=!arg!
  ) else (
    set arg='%%a' 
    set ARGV=!ARGV!,!arg!
  )

rem RUBYOPT が指定されているとエラーになるため RUBYOPT を unset する
set RUBYOPT=

!JRUBY_CMD! -e "require '!RTA_HOME!\lib\rta'; RTA::Controller::Runner.run(!ARGV!)"
