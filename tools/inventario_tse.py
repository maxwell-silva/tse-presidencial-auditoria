#!/usr/bin/env python3
"""Inventário de fontes públicas do TSE. Não baixa arquivos do acervo."""

import argparse
import http.client
import json
import re
import shutil
import time
import unicodedata
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

YEARS = (2018, 2022, 2026)
CKAN = "https://dadosabertos.tse.jus.br"
RESULTS = "https://resultados.tse.jus.br"
EA11 = f"{RESULTS}/oficial/comum/config/ele-c.json"
HOSTS = {"dadosabertos.tse.jus.br", "cdn.tse.jus.br", "resultados.tse.jus.br"}
UFS = set("AC AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO ZZ BR".split())
ROLES = {"1": "presidente", "3": "governador"}
RETRY_HTTP = {408, 429, 500, 502, 503, 504}
DOWNLOAD_TYPES = {".zip", ".json", ".csv", ".txt", ".pdf", ".sha1", ".sha2", ".sha256", ".sha512"}
BLOCK = 1024**2
RESERVE = 5 * 1024**3
SMOKE_LIMIT = 8 * BLOCK


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def normalize(value):
    return "".join(c for c in unicodedata.normalize("NFD", value.lower())
                   if not unicodedata.combining(c))


def official(url):
    parts = urlsplit(url)
    if parts.scheme != "https" or parts.netloc.lower() not in HOSTS:
        raise ValueError(f"URL fora das origens HTTPS cadastradas: {url}")
    return url


class Network(HTTPRedirectHandler):
    def __init__(self, delay):
        self.delay = delay
        self.next_request = 0.0
        self.opener = build_opener(self)

    def pace(self):
        time.sleep(max(0.0, self.next_request - time.monotonic()))
        self.next_request = time.monotonic() + self.delay

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        official(newurl)
        self.pace()
        return super().redirect_request(req, fp, code, msg, headers, newurl)

    def request(self, url, consume, method="GET"):
        req = Request(official(url), method=method, headers={
            "User-Agent": "DarkChannels-Coleta-TSE/1.0",
            "Accept-Encoding": "identity",
        })
        for attempt in range(4):
            self.pace()
            retry_after = 0
            try:
                with self.opener.open(req, timeout=40) as response:
                    return consume(response)
            except HTTPError as exc:
                if exc.code not in RETRY_HTTP or attempt == 3:
                    raise
                retry_after = retry_seconds(exc.headers.get("Retry-After"))
                exc.close()
            except (URLError, TimeoutError, ConnectionError, http.client.IncompleteRead, TransferError):
                if attempt == 3:
                    raise
            time.sleep(max(retry_after, 2 ** (attempt + 1)))

    def json(self, url):
        def read(response):
            data = response.read(12 * BLOCK + 1)
            if len(data) > 12 * BLOCK:
                raise ValueError(f"Metadado maior que 12 MiB: {url}")
            return json.loads(data)

        return self.request(url, read)


class TransferError(OSError):
    """Resposta truncada ou transitória."""


def retry_seconds(value):
    if not value:
        return 0
    try:
        return max(0, int(value))
    except ValueError:
        try:
            return max(0, (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds())
        except (ValueError, TypeError, OverflowError):
            return 0


@dataclass(frozen=True)
class Resource:
    year: int
    category: str
    path: str
    url: str
    source: str
    title: str
    size: int | None = None
    expected: dict | None = None


def resource(year, category, parts, url, source, title, size=None, expected=None):
    filename = unquote(urlsplit(official(url)).path.rsplit("/", 1)[-1])
    if not re.fullmatch(r"[\w.-]+", filename) or filename in {".", ".."}:
        raise ValueError(f"Nome de arquivo inválido: {url}")
    return Resource(year, category, "/".join((str(year), category, *parts, filename)),
                    url, source, title, size, expected)


def ckan_rows(network, year):
    query = urlencode({"q": f"title:{year}", "rows": 100})
    body = network.json(f"{CKAN}/api/3/action/package_search?{query}")
    found = body["result"]
    if not body["success"] or found["count"] > len(found["results"]):
        raise ValueError(f"Pesquisa CKAN incompleta para {year}")
    return found["results"]


def ckan_resource(year, package, row):
    slug = package["name"]
    title = row["name"].strip()
    plain = normalize(title)
    url = row["url"]
    filename = unquote(urlsplit(url).path.rsplit("/", 1)[-1])
    suffix = Path(filename).suffix.lower()
    if suffix not in DOWNLOAD_TYPES:
        return None
    uf = title.split(" - ", 1)[0].upper()
    if uf not in UFS:
        match = re.search(r"_([A-Z]{2})(?:_|\.)", filename.upper())
        uf = match[1] if match and match[1] in UFS else "BR"
    match = re.search(r"primeiro turno|\b1[ºo°] turno|\b1t\b|_1t_|segundo turno|\b2[ºo°] turno|\b2t\b|_2t_",
                      f"{plain} {slug} {filename.lower()}")
    turn = "turno-1" if match and match[0].strip("_").startswith(("1", "primeiro")) else (
        "turno-2" if match else "turnos-no-arquivo")
    level = "brasil" if uf == "BR" else "uf"
    scope = (level, uf) if level == "uf" else (level,)
    if slug == f"resultados-{year}":
        if "historico totalizacao" in plain:
            if "presidente" not in plain and "governador" not in plain:
                return None
            category, granularity = "historico-totalizacao", "agregado"
        elif "relatorio de totalizacao" in plain:
            category, granularity = "relatorio-totalizacao", "agregado"
        elif "secao" in plain:
            category, granularity = "resultados", "secao"
        elif "municipio e zona" in plain:
            category, granularity = "resultados", "municipio-zona"
        else:
            return None
    elif slug == f"resultados-{year}-boletim-de-urna":
        category, granularity = "boletim-urna", "secao"
    elif slug == f"resultados-{year}-arquivos-transmitidos-para-totalizacao":
        category, granularity = "arquivos-transmitidos", "urna-secao"
    elif slug.startswith(f"resultados-{year}-correspondencias-"):
        category, granularity = "correspondencias", "urna-secao"
    elif slug.startswith(f"resultados-{year}-logs-do-sistema-"):
        category, granularity = "logs-gedai", "preparacao-urna"
    elif slug == f"eleitorado-{year}":
        category = "eleitorado"
        granularity = "secao" if "secao" in plain or "secoes" in plain else (
            "local-votacao" if "local de votacao" in plain else "perfil")
    elif slug == f"comparecimento-e-abstencao-{year}":
        category, granularity = "comparecimento-abstencao", "perfil"
    elif slug == f"candidatos-{year}" and plain == "candidatos":
        category, granularity = "candidaturas", "cadastro"
    else:
        return None
    size = row.get("size")
    size = size if isinstance(size, int) and size > 0 else None
    return resource(year, category, (granularity, turn, *scope), url,
                    f"{CKAN}/dataset/{slug}/resource/{row['id']}", title, size)


def result_catalog(network, year, cfg):
    resources = []
    elections = {}
    for poll in cfg["pl"]:
        if poll["c"] != f"ele{year}" or not poll["dt"].endswith(f"/{year}"):
            continue
        for election in poll["e"]:
            roles = {str(cp["cd"]) for region in election["abr"] for cp in region["cp"]}
            for role in ROLES.keys() & roles:
                if "ordinaria" in normalize(election["nm"]):
                    elections[role, int(election["t"])] = (poll, election)
    if not elections:
        return resources
    if set(elections) != {(role, turn) for role in ROLES for turn in (1, 2)}:
        raise ValueError(f"EA11 sem os quatro pares cargo/turno para {year}")
    resources.append(resource(year, "catalogos", ("ea11",), EA11, EA11, "Configuração oficial EA11"))
    layouts = {x["tp"]: x["dir"] for x in cfg["arq"]}
    for (role, turn), (poll, election) in sorted(elections.items()):
        code = election["cd"]
        if not re.fullmatch(r"\d{1,6}", code):
            raise ValueError(f"Código eleitoral inválido: {code}")
        prefix = (ROLES[role], f"turno-{turn}")
        tokens = {"<base>": RESULTS, "<ambiente>": "oficial", "<ciclo>": poll["c"],
                  "<cd_eleicao>": code, "<cd_pleito>": poll["cd"]}

        def directory(kind, uf="br"):
            path = layouts[kind].replace("<uf>", uf)
            for key, value in tokens.items():
                path = path.replace(key, value)
            return path

        cm_url = f"{directory('cm')}/mun-e{int(code):06d}-cm.json"
        cm = network.json(cm_url)
        resources.append(resource(year, "catalogos", ("ea12", *prefix), cm_url, EA11, "Municípios e zonas EA12"))

        def add(uf, code_geo, area, parts):
            place = uf if area in ("br", "uf") else f"{uf}{code_geo}"
            url = f"{directory('u', uf)}/{place}-c{int(role):04d}-e{int(code):06d}-u.json"
            expected = {"ele": code, "t": str(turn), "tpabr": area, "cdabr": code_geo,
                        "cargo": role}
            resources.append(resource(year, "resultado-ea20", (*prefix, *parts), url, cm_url,
                                      f"EA20 {ROLES[role]} T{turn} {area} {code_geo}", expected=expected))

        def add_monitor(uf, parts):
            url = f"{directory('ab', uf)}/{uf}-e{int(code):06d}-ab.json"
            expected = {"ele": code, "t": str(turn)}
            resources.append(resource(year, "acompanhamento", (*prefix, *parts), url, cm_url,
                                      f"Acompanhamento T{turn} {uf}", expected=expected))

        if role == "1":
            add("br", "br", "br", ("brasil",))
        add_monitor("br", ("ea14", "brasil"))
        for region in cm["abr"]:
            uf = region["cd"]
            if uf.upper() not in UFS - {"BR"} or (uf == "zz" and role == "3"):
                continue
            add(uf, uf, "uf", ("uf", uf.upper()))
            add_monitor(uf, ("ea15", "uf", uf.upper()))
            if role == "1":
                # EA16 é do pleito inteiro e é comum aos cargos.
                cs_url = f"{directory('cs', uf)}/{uf}-p{int(poll['cd']):06d}-cs.json"
                resources.append(resource(year, "catalogos", ("ea16", f"turno-{turn}", "uf", uf.upper()),
                                          cs_url, EA11, f"Seções eleitorais EA16 T{turn} {uf}"))
            for city in region["mu"]:
                city_code = city["cd"]
                if not re.fullmatch(r"\d{5}", city_code):
                    raise ValueError(f"Código municipal inválido: {city_code}")
                add(uf, city_code, "mu", ("municipio", uf.upper(), city_code))
    return resources


def atomic_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_suffix(path.suffix + ".part")
    part.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    part.replace(path)


def build_plan(network, root):
    catalog = network.json(EA11)
    resources = {}
    coverage = {}
    for year in YEARS:
        items = []
        packages = ckan_rows(network, year)
        for package in packages:
            for row in package["resources"]:
                item = ckan_resource(year, package, row)
                if item:
                    items.append(item)
        items.extend(result_catalog(network, year, catalog))
        counts = dict(Counter(item.category for item in items))
        if not all(counts.get(category) for category in ("boletim-urna", "resultados", "correspondencias")):
            raise ValueError(f"Cobertura CKAN inesperada para {year}: {counts}")
        coverage[year] = counts
        for item in items:
            old = resources.setdefault(item.path, item)
            if old.url != item.url:
                raise ValueError(f"Recursos diferentes apontam para {item.path}")
        print(year, counts, flush=True)
    path = root / "_meta" / "plano.json"
    atomic_json(path, {
        "created_at": now(), "destination": str(root), "available_bytes": shutil.disk_usage(root).free,
        "coverage": coverage,
        "scope": "Presidente e Governador. Os ZIPs conjuntos conservam os demais cargos na origem.",
        "gaps": [
            "EA20 histórico depende do catálogo oficial vigente; 2018/2022 têm resultados CKAN.",
            "ZIP público com log/RDV por urna encontrado no CKAN para 2022 e 2026.",
            "EA20 por zona, EA18 e arquivos avulsos por urna não entram neste plano; "
            "resultados por zona/seção e arquivos de urna entram pelos pacotes CKAN disponíveis.",
            "EA10 documentado, mas rotas oficiais de exemplo em ambos os turnos retornaram 404; "
            "o EA20 e os resultados CKAN preservam a situação de cada candidatura onde publicada.",
            "Micro/meso/macrorregiões exigem cruzamento geográfico municipal do IBGE.",
        ],
        "resources": [asdict(item) for item in sorted(resources.values(), key=lambda item: item.path)],
    })
    print(f"Plano gravado: {path} | {len(resources)} recursos", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    build_plan(Network(0.75), root)


if __name__ == "__main__":
    main()
