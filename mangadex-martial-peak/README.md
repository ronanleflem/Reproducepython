# Martial Peak — téléchargement MangaDex (v1)

MangaDex expose une [API publique](https://api.mangadex.org/docs/) : pour **Martial Peak** en anglais, il y a environ **3900+ chapitres** (`translatedLanguage=en`).

Titre : [Martial Peak sur MangaDex](https://mangadex.org/title/b1461071-bfbb-43e7-a5b6-a7ba5904649f)

## v1 — un chapitre

Prérequis : Python 3.10+ (stdlib uniquement).

```bash
cd mangadex-martial-peak
python3 download_chapter.py --chapter 1
python3 download_chapter.py --chapter 1531
```

Les pages vont dans `downloads/martial-peak/chapter-<num>/` (`001.jpg`, …, plus `meta.json` local).  
Les téléchargements complets restent **hors git** (`.gitignore`).

**Échantillon dans le repo** (pour tester le rendu) :  
[`sample/martial-peak-ch1531-page001.jpg`](sample/martial-peak-ch1531-page001.jpg) — page 1 du chapitre 1531 EN.

### Chapitres testés

| Chapitre | Pages | ID MangaDex | Notes |
|----------|------:|-------------|--------|
| 1 | 20 | `f5cb46fa-eceb-40ae-a5ea-a5c28f47c2a0` | premier chapitre EN |
| 1531 | 16 | `f9ce4e45-3f65-41f5-b8a2-ae20ab963a27` | titre MD : « Do You Want to Try » |

Options utiles :

- `--chapter-id UUID` — télécharger directement par ID MangaDex
- `--data-saver` — images plus légères
- `--out chemin/` — autre dossier de sortie

Respecte un petit délai entre les pages (`--delay`, défaut 0,35 s) pour limiter la charge sur l’API.

## v2 (à venir)

Téléchargement par lot (plage de chapitres, reprise, parallélisme modéré).
