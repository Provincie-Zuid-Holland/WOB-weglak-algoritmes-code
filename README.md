<p align="center">
  <img src="images/PZH_Basislogo.svg" width="50%">
</p>

## Inleiding

In het kader van het WOB/WOO hebben wij algoritmes ontwikkeld om persoonsgegevens te identificeren in documenten. Geïdentificeerd gegevens kunnen worden gebruikt om documenten te markeren en weg te lakken in Adobe Acrobat.
Het bestandstype van de documenten moet wel pdf zijn. Om documenten naar pdf te converteren kan de [PDF-conversion-tool](https://github.com/Provincie-Zuid-Holland/PDF-conversion-tool) gebruikt worden. 

De volgende persoonsgegevens kunnen worden herkend:
- Email adressen
- Straatnamen met huisnummers en postcodes
- Voor‐ en achternamen
- Geldbedragen
- KvK‐nummer, IBAN, BRS‐nummer, telefoonnummers
- Persoonlijk beleidsopvattingen (indicatie van waar het in de tekst zou kunnen zijn)

Twee Databricks notebooks zijn gebruikt om verschillende stappen te verrichten:
- Notebook1_ocr_tika.py: 
	- OCR (Optical Character Recognition) kan gedaan worden om afbeeldingen over te zetten naar tekst
	- parsen is gebruikt on tekst in pdf's te herkennen en deze op te slaan als .txt
- Notebook2_weglakken.py: in deze notebook worden op basis van verschillende regex (reguliere expressies) persoonsgegevens herkent. Een dictionary wordt gemaakt van alle herkent gegevens. Bestanden voor het markeren van de pdf documenten in Adobe Acrobat en voor het installeren van de juiste adobe settings worden ook gecreëerd.

## Persoonsgegevens herkennen in Databricks
Voor de vereiste libraries en init script zie de map 'resources'.

### OCR en tika parsen
Ga naar Notebook1_ocr_tika.py. Vul de juiste parameters in.

<p align="center">
  <img src="images/parameters.PNG">
</p>

Run de ETL steps op de pdf documenten en check dat parsen is gedaan voor alle documenten. Herhaal als nodig de OCR en dan de tika parsen.
Er wordt een nieuwe map 'tika_parsed' gemaakt in de 'process directory' (process_dir) met alle gevonden documenten als .txt bestanden.

### Gegevens herkennen en dictionary maken
In Notebook2_weglakken.py wordt een dictionary met herkent persoonsgegevens gemaakt. Ter controle is de dictionary doorgestuurd aan het wob team voordat documenten gemarkeerd worden. Het wob team geeft dan feedback (toevoegen en weghalen van gegevens) en eventueel wordt de dictionary aangepast.
Deze notebook kan meteen worden gebruikt vanaf Notebook1_ocr_tika.py'. Om de notebook zelf te runnen moeten de parameters aangepast worden.
Als resultaat wordt er de map 'Output_ProDC' gemaakt in de 'process directory' met de benodigde bestanden (inclusief de Adobe dictionary) voor het markeren van de PDF's in Adobe Acrobat.

## PDF's markeren in Adobe Acrobat Pro DC

**Let op:** plug-in AutoRedact is nodig.

### AutoRedact configureren
Om de AutoRedact te configureren zo dat een lijst met wob uitzondering gronden en verschillende AutoRedact settings in meerdere computers snel gebruikt kunnen worden volg de instructie in map 'AutoRedact configuratie'.

### Bestanden downloaden en installeren
Installeer de dictionary en wizard handeling in je lokale adobe settings door naar de map 'Output_ProDC' te gaan en 'bestanden_installeren.bat' te runnen.

<p align="center">
  <img src="images/adobe_bestanden.png">
</p>

### Bestanden markeren met Autobatch

**Let op:** plug-in Autobatch is nodig.

Run het bestand 'AutoBatch.bat'. 'AutoBatch.bat' gaat Adobe Acrobat Pro DC aan de achterkant openen, de bestanden markeren en opslaan, en Adobe closen. Deze kan op twee manieren gedraaid worden. 
Dubbelklik op 'AutoBatch.bat' om handmatig individuele bestanden te selecteren voor markering. Gemarkeerde bestanden worden in de map met de originele bestanden opgeslagen (originele bestanden worden vervangen). 

'AutoBatch.bat' kan ook vanaf de opdrachtprompt gedraaid worden. Met deze optie kan ook een map met eventueel sub-mappen geselecteerd worden. De gemarkeerde bestanden worden dan opgeslagen in de originele mappen (originele bestanden worden vervangen).

<p align="center">
  <img src="images/autobatch_cmd.png" width="50%">
</p>

Het is ook mogelijk om de gemarkeerde bestanden in een specifieke map op te slaan (deze moet wel een bestaande map zijn). Met deze optie worden alle bestanden (ook die van sub-mappen) in de dezelfde map opgeslagen.

<p align="center">
  <img src="images/autobatch_cmd2.png" width="75%">
</p>

### Bestanden markeren zonder Autobatch
Open Adobe Acrobat Pro DC en ga naar 'Gereedschappen'. Klik op 'Wizard Handelingen' en selecteer de geïnstalleerd handeling. Klik op 'Bestanden Toevoegen' om een map of losse bestanden toe te voegen en klik daarna op begin. De documenten worden een per een geopend, gemarkeerd en opgeslagen.

## Weglakken in Adobe Acrobat Pro DC
Voor het werken met gemarkeerde bestanden in Adobe Acrobat Pro DC zie de [handleiding](https://github.com/Provincie-Zuid-Holland/WOB-weglak-algoritmes-code/tree/main/handleiding).

## Auteurs
- Joana Cardoso
- Michael de Winter
- Dennis van Muijen

## Contact
Voor vragen of opmerkingen graag contact opnemen met vdwh@pzh.nl.