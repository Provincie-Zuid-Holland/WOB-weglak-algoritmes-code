@echo off
rem Kopieert autoredact dict naar appdata folder van gebruiker
echo Wilt u deze bestanden installeren? Bestaande dictionaries en wizard handelingen worden hiermee overschreven. Druk op ENTER op door te gaan.
pause >nul

copy bestanden\AutoRedactCodesLocal.cfs %AppData%\Evermap\AutoRedact\AutoRedactCodesLocal.cfs
copy bestanden\AutoRedact.cfs %AppData%\Evermap\AutoRedact\AutoRedact.cfs
copy bestanden\{{wob_project}}.cfs %AppData%\Evermap\AutoRedact\AutoRedactDictLocal.cfs

if not exist "%AppData%\Adobe\Acrobat\10.0\Sequences" mkdir "%AppData%\Adobe\Acrobat\10.0\Sequences"
copy bestanden\{{wob_project}}.sequ %AppData%\Adobe\Acrobat\10.0\Sequences\{{wob_project}}.sequ

echo Druk op ENTER om af te sluiten.
pause >nul