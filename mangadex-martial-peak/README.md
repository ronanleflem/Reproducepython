# Martial Peak — téléchargement MangaDex (v1)

MangaDex expose une [API publique](https://api.mangadex.org/docs/) : pour **Martial Peak** en anglais, il y a environ **3900+ chapitres** (`translatedLanguage=en`).

Titre : [Martial Peak sur MangaDex](https://mangadex.org/title/b1461071-bfbb-43e7-a5b6-a7ba5904649f)

## v1 — un chapitre

Prérequis : Python 3.10+ (stdlib uniquement).

```bash
cd mangadex-martial-peak
python3 download_chapter.py --chapter 1
```

Les pages vont dans `downloads/martial-peak/chapter-1/` (`001.jpg`, …, plus `meta.json`).

Options utiles :

- `--chapter-id UUID` — télécharger directement par ID MangaDex
- `--data-saver` — images plus légères
- `--out chemin/` — autre dossier de sortie

Respecte un petit délai entre les pages (`--delay`, défaut 0,35 s) pour limiter la charge sur l’API.

## v2 (à venir)

Téléchargement par lot (plage de chapitres, reprise, parallélisme modéré).
