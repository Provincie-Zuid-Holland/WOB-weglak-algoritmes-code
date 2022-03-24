@echo off
rem Kopieert autoredact dict naar appdata folder van gebruiker
echo Wilt u deze bestanden installeren? Bestaande dictionaries en wizard handelingen worden hiermee overschreven. Druk op ENTER op door te gaan.
pause >nul

copy bestanden\{{wob_project}}.cfs %AppData%\Evermap\AutoRedact\AutoRedactDictLocal.cfs

if not exist "%AppData%\Adobe\Acrobat\DC\Sequences" mkdir "%AppData%\Adobe\Acrobat\DC\Sequences"
copy bestanden\{{wob_project}}.sequ %AppData%\Adobe\Acrobat\DC\Sequences\{{wob_project}}.sequ

echo Druk op ENTER om af te sluiten.
pause >nul