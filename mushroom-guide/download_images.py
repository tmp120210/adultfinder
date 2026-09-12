#!/usr/bin/env python3
import json
import os
import ssl
import time
import urllib.parse
import urllib.request

ctx = ssl.create_default_context()

species = [
    ("boletus-edulis", "Boletus edulis"),
    ("leccinum-aurantiacum", "Leccinum aurantiacum"),
    ("leccinum-scabrum", "Leccinum scabrum"),
    ("suillus-luteus", "Suillus luteus"),
    ("suillus-granulatus", "Suillus granulatus"),
    ("suillus-bovinus", "Suillus bovinus"),
    ("imleria-badia", "Imleria badia"),
    ("cantharellus-cibarius", "Cantharellus cibarius"),
    ("craterellus-tubaeformis", "Craterellus tubaeformis"),
    ("lactarius-deliciosus", "Lactarius deliciosus"),
    ("lactarius-resimus", "Lactarius resimus"),
    ("lactarius-torminosus", "Lactarius torminosus"),
    ("russula-aeruginea", "Russula aeruginea"),
    ("armillaria-mellea", "Armillaria mellea"),
    ("hydnum-repandum", "Hydnum repandum"),
    ("morchella-elata", "Morchella elata"),
    ("tylopilus-felleus", "Tylopilus felleus"),
    ("chalciporus-piperatus", "Chalciporus piperatus"),
    ("paxillus-involutus", "Paxillus involutus"),
    ("amanita-muscaria", "Amanita muscaria"),
]

UA = {"User-Agent": "MushroomGuideBot/1.0 (educational; local guide)"}


def api(params):
    params = dict(params)
    params["format"] = "json"
    url = "https://commons.wikimedia.org/w/api.php?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
        return json.load(r)


def search_files(query, limit=20):
    data = api(
        {
            "action": "query",
            "list": "search",
            "srsearch": query,
            "srnamespace": 6,
            "srlimit": limit,
        }
    )
    return [x["title"] for x in data.get("query", {}).get("search", [])]


def file_info(titles):
    if not titles:
        return {}
    data = api(
        {
            "action": "query",
            "titles": "|".join(titles[:40]),
            "prop": "imageinfo",
            "iiprop": "url|mime|size|extmetadata",
            "iiurlwidth": 900,
        }
    )
    out = {}
    for page in data.get("query", {}).get("pages", {}).values():
        title = page.get("title")
        infos = page.get("imageinfo") or []
        if not infos:
            continue
        ii = infos[0]
        mime = ii.get("mime", "")
        if not mime.startswith("image/") or mime == "image/svg+xml":
            continue
        out[title] = {
            "thumb": ii.get("thumburl") or ii.get("url"),
            "url": ii.get("url"),
            "mime": mime,
            "license": (ii.get("extmetadata") or {})
            .get("LicenseShortName", {})
            .get("value", ""),
        }
    return out


def download(url, path):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, context=ctx, timeout=60) as r:
        data = r.read()
    with open(path, "wb") as f:
        f.write(data)


def score_cap(title, name):
    tl = title.lower()
    s = 0
    for w in ["habitat", "forest", "group", "stand", "in situ", name.lower().split()[0]]:
        if w in tl:
            s += 1
    for w in [
        "spore",
        "micro",
        "dish",
        "cooked",
        "illustration",
        "drawing",
        "stamp",
        "map",
        "distribution",
        "icon",
    ]:
        if w in tl:
            s -= 5
    return s


def score_detail(title):
    tl = title.lower()
    s = 0
    for w in [
        "underside",
        "pore",
        "pores",
        "gill",
        "gills",
        "stem",
        "stipe",
        "lamella",
        "hymen",
        "tube",
        "bottom",
        "below",
        "schnitt",
        "cut",
        "section",
        "detail",
        "close",
    ]:
        if w in tl:
            s += 3
    for w in [
        "spore",
        "micro",
        "dish",
        "cooked",
        "illustration",
        "drawing",
        "stamp",
        "map",
        "distribution",
        "icon",
    ]:
        if w in tl:
            s -= 5
    return s


def main():
    os.makedirs("images", exist_ok=True)
    manifest = {}

    for slug, name in species:
        print("===", name)
        titles = []
        try:
            cat = api(
                {
                    "action": "query",
                    "list": "categorymembers",
                    "cmtitle": f"Category:{name}",
                    "cmtype": "file",
                    "cmlimit": 50,
                }
            )
            titles = [
                x["title"] for x in cat.get("query", {}).get("categorymembers", [])
            ]
        except Exception as e:
            print("cat err", e)

        if len(titles) < 10:
            titles += search_files(f"{name}", 25)

        seen = set()
        uniq = []
        for t in titles:
            if t not in seen:
                seen.add(t)
                uniq.append(t)

        info = {}
        for i in range(0, min(len(uniq), 40), 40):
            info.update(file_info(uniq[i : i + 40]))

        items = list(info.items())
        if not items:
            print("NO IMAGES", name)
            continue

        caps = sorted(items, key=lambda x: score_cap(x[0], name), reverse=True)
        details = sorted(items, key=lambda x: score_detail(x[0]), reverse=True)

        pick1 = caps[0]
        pick2 = None
        for d in details:
            if d[0] != pick1[0]:
                pick2 = d
                break
        if not pick2 and len(caps) > 1:
            pick2 = caps[1]
        if not pick2:
            pick2 = pick1

        # Prefer two visually different picks: if detail score of pick2 is poor, take 2nd/3rd cap
        if score_detail(pick2[0]) <= 0 and len(caps) > 2:
            for c in caps[1:4]:
                if c[0] != pick1[0]:
                    pick2 = c
                    break

        entry = {"name": name, "images": []}
        for idx, (title, meta) in enumerate([pick1, pick2], 1):
            ext = ".jpg"
            if "png" in meta["mime"]:
                ext = ".png"
            elif "webp" in meta["mime"]:
                ext = ".webp"
            path = f"images/{slug}-{idx}{ext}"
            try:
                download(meta["thumb"], path)
                entry["images"].append(
                    {
                        "file": path,
                        "source_title": title,
                        "source_url": meta["url"],
                        "license": meta["license"],
                    }
                )
                print(" saved", path, "<-", title[:70])
            except Exception as e:
                print(" dl fail", title, e)
            time.sleep(0.25)
        manifest[slug] = entry
        time.sleep(0.35)

    with open("manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    print("DONE", len(manifest))


if __name__ == "__main__":
    main()
