# author_inst_edges_simple.py — build co-citation edges (authors & institutions) on-the-fly
import os, json, re, time, requests, pandas as pd
from itertools import combinations
from collections import defaultdict

DATA_FOLDER = "/Users/denggeyileao/Desktop/metadata_include"  
FOCUS_FIELD = "referenced_works"
OPENALEX = "https://api.openalex.org/works/"
DELAY = 0.3
TIMEOUT = 15

AUTHOR_NODES = "author_nodes.csv"
INSTITUTION_NODES = "institution_nodes.csv"
AUTHOR_EDGES_OUT = "author_edges.csv"
INSTITUTION_EDGES_OUT = "institution_edges.csv"
MIN_WEIGHT = 1 

def norm(s):
    if not isinstance(s, str): return ""
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE).lower().strip()
    return re.sub(r"\s+", " ", s)

def work_id(x):  # 支持 URL 或 Wxxxx
    return (x or "").split("/")[-1].strip()

def extract_orcid(url):
    if not url: return ""
    m = re.search(r"(\d{4}-\d{4}-\d{4}-\d{3}[0-9X])", url)
    return m.group(1) if m else ""

def s_safe(x): 
    return "" if pd.isna(x) else str(x).strip()

def author_key(orcid, openalex_id, name_norm):
    return f"orcid:{orcid}" if orcid else (f"id:{openalex_id}" if openalex_id else f"name:{name_norm}")

def inst_key(openalex_inst_id, name_norm):
    return f"id:{openalex_inst_id}" if openalex_inst_id else f"name:{name_norm}"

dfA = pd.read_csv(AUTHOR_NODES)
dfI = pd.read_csv(INSTITUTION_NODES)

author_map = {
    author_key(
        s_safe(r.get("ORCID")),
        s_safe(r.get("OpenAlexAuthorID")),
        s_safe(r.get("NormName")),
    ): int(r["ID"])
    for _, r in dfA.iterrows()
}

inst_map = {
    inst_key(
        s_safe(r.get("OpenAlexInstitutionID")),
        s_safe(r.get("NormName")),
    ): int(r["ID"])
    for _, r in dfI.iterrows()
}

print(f"authors csv rows: {len(dfA)}, unique keys loaded: {len(author_map)}")
print(f"institutions csv rows: {len(dfI)}, unique keys loaded: {len(inst_map)}")

cache = {}
sess = requests.Session()
sess.headers.update({"User-Agent": "ThesisCoCitation/1.0"})

def resolve_wid(wid):
    """返回 (author_id 或 None, inst_id 或 None)，用简单缓存避免重复请求"""
    if wid in cache:
        return cache[wid]
    try:
        r = sess.get(OPENALEX + wid, timeout=TIMEOUT)
        if r.status_code != 200:
            time.sleep(DELAY); cache[wid] = (None, None); return cache[wid]
        w = r.json()
    except Exception:
        time.sleep(DELAY); cache[wid] = (None, None); return cache[wid]

    auths = w.get("authorships", []) or []
    if not auths:
        cache[wid] = (None, None); return cache[wid]

    first_auth = next((au for au in auths if au.get("author_position") == "first"), auths[0])
    a = (first_auth.get("author") or {})
    a_name = a.get("display_name") or ""
    a_orcid = extract_orcid(a.get("orcid", ""))
    a_oid = a.get("id") or ""
    a_k = author_key(a_orcid, a_oid, norm(a_name)) if a_name else None
    a_id = author_map.get(a_k) if a_k else None

    rep_inst = None
    firsts = [au for au in auths if au.get("author_position") == "first"]
    ordered = [firsts[0]] + [au for au in auths if au is not firsts[0]] if firsts else auths
    for au in ordered:
        insts = au.get("institutions") or []
        if insts:
            cand = insts[0]
            iname = (cand.get("display_name") or "").strip()
            if iname:
                rep_inst = cand
                break
    if rep_inst:
        iid = rep_inst.get("id") or ""
        i_k = inst_key(iid, norm(rep_inst.get("display_name") or ""))
        i_id = inst_map.get(i_k)
    else:
        i_id = None

    cache[wid] = (a_id, i_id)
    return cache[wid]

files = sorted([f for f in os.listdir(DATA_FOLDER) if f.endswith(".json")])
author_edges = defaultdict(int)   # (u,v) -> weight
inst_edges = defaultdict(int)

last_reported = 0
for fname in files:
    fpath = os.path.join(DATA_FOLDER, fname)
    try:
        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        continue

    a_set, i_set = set(), set()
    for w in data.get(FOCUS_FIELD, []) or []:
        wid = work_id(w)
        if not wid: continue
        a_id, i_id = resolve_wid(wid)
        if len(cache) // 100 > last_reported:
            last_reported = len(cache) // 100
            print(f"fetched {len(cache)} unique works")
        if a_id: a_set.add(int(a_id))
        if i_id: i_set.add(int(i_id))

    for u, v in combinations(sorted(a_set), 2):
        author_edges[(u, v)] += 1
    for u, v in combinations(sorted(i_set), 2):
        inst_edges[(u, v)] += 1

    time.sleep(DELAY)

def to_df(ed):
    rows = [{"Source": u, "Target": v, "Weight": w}
            for (u, v), w in ed.items() if w >= MIN_WEIGHT]
    df = pd.DataFrame(rows)
    return df.sort_values(["Weight", "Source", "Target"], ascending=[False, True, True]) if not df.empty else df

df_auth = to_df(author_edges)
df_inst = to_df(inst_edges)
df_auth.to_csv(AUTHOR_EDGES_OUT, index=False)
df_inst.to_csv(INSTITUTION_EDGES_OUT, index=False)

print(f"author edges: {len(df_auth)} -> {AUTHOR_EDGES_OUT}")
print(f"institution edges: {len(df_inst)} -> {INSTITUTION_EDGES_OUT}")
