@echo off
rem Kopieert autoredact config files naar appdata folder van gebruiker
echo Wilt AutoRedact (opnieuw) configureren? Bestaande instellingen worden hiermee overschreven. Druk op ENTER op door te gaan.
pause >nul

copy bestanden\AutoRedactCodesLocal.cfs %AppData%\Evermap\AutoRedact\AutoRedactCodesLocal.cfs
copy bestanden\AutoRedact.cfs %AppData%\Evermap\AutoRedact\AutoRedact.cfs

echo Druk op ENTER om af te sluiten.
pause >nul