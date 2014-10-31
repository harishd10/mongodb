@ECHO OFF
SET VERSION=2.4.0
SET BINDIR=..\..\..\build\win32\normal\mongo
SET LICENSEDIR=..\..\..\distsrc

:loop
IF NOT "%1"=="" (
    IF "%1"=="-version" (
        SET VERSION=%2
        SHIFT
    )
    IF "%1"=="-bindir" (
        SET BINDIR=%2
        SHIFT
    )
    IF "%1"=="-licensedir" (
        SET LICENSEDIR=%2
        SHIFT
    )
    SHIFT
    GOTO :loop
)

ECHO Building msi for version %VERSION% with binaries from %BINDIR% and license files from %LICENSEDIR%
msbuild /p:Configuration=Release;Version=%VERSION%;License=%LICENSEDIR%;Source=%BINDIR% MongoDB.wixproj