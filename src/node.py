# author_inst_first_only.py  -- co-citation nodes (first author & first institution, with fallback to next author for institution)
import os, json, re, time, requests, pandas as pd

DATA_FOLDER = "/Users/denggeyileao/Desktop/metadata_include" 
FOCUS_FIELD = "referenced_works"         
OUTPUT_AUTHOR = "author_nodes.csv"
OUTPUT_INST = "institution_nodes.csv"
OPENALEX = "https://api.openalex.org/works/"
DELAY = 0.3       

def norm(s):
    if not isinstance(s, str): return ""
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE).lower().strip()
    return re.sub(r"\s+", " ", s)

def work_id(x): 
    return (x or "").split("/")[-1].strip()

def extract_orcid(url):
    if not url: return ""
    m = re.search(r"(\d{4}-\d{4}-\d{4}-\d{3}[0-9X])", url)
    return m.group(1) if m else ""

target_ids = set()
files = sorted([f for f in os.listdir(DATA_FOLDER) if f.endswith(".json")])

for fname in files:
    fpath = os.path.join(DATA_FOLDER, fname)
    try:
        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        continue
    for w in data.get(FOCUS_FIELD, []):
        wid = work_id(w)
        if wid: target_ids.add(wid)

target_ids = sorted(target_ids)
print("unique referenced works:", len(target_ids))
if not target_ids:
    raise SystemExit("No IDs found. Check DATA_FOLDER or FOCUS_FIELD.")

authors, insts = [], []
author_seen, inst_seen = set(), set() 

for idx, wid in enumerate(target_ids, 1):
    try:
        r = requests.get(OPENALEX + wid, timeout=15)
        if r.status_code != 200:
            time.sleep(DELAY); continue
        w = r.json()
    except Exception:
        time.sleep(DELAY); continue

    auths = w.get("authorships", []) or []
    if not auths:
        time.sleep(DELAY); continue

    first_auth = next((au for au in auths if au.get("author_position") == "first"), auths[0])

    a = (first_auth.get("author") or {})
    name = a.get("display_name") or ""
    if name:
        orcid = extract_orcid(a.get("orcid", ""))
        aid = a.get("id") or ""  # 形如 'https://openalex.org/A123...'
        akey = f"orcid:{orcid}" if orcid else (f"id:{aid}" if aid else f"name:{norm(name)}")
        if akey not in author_seen:
            author_seen.add(akey)
            authors.append({
                "ID": len(author_seen),
                "Author": name,
                "ORCID": orcid,
                "OpenAlexAuthorID": aid,
                "NormName": norm(name)
            })
            
    rep_inst = None

    firsts = [au for au in auths if au.get("author_position") == "first"]
    if firsts:
        ordered_auths = [firsts[0]] + [au for au in auths if au is not firsts[0]]
    else:
        ordered_auths = auths

    for au in ordered_auths:
        inst_list = au.get("institutions") or []
        if inst_list:
            candidate = inst_list[0]
            iname = (candidate.get("display_name") or "").strip()
            if iname:
                rep_inst = candidate
                break

    if rep_inst:
        iname = rep_inst.get("display_name") or ""
        iid = rep_inst.get("id") or ""  # 形如 'https://openalex.org/I123...'
        ikey = f"id:{iid}" if iid else f"name:{norm(iname)}"
        if ikey not in inst_seen:
            inst_seen.add(ikey)
            insts.append({
                "ID": len(inst_seen),
                "Institution": iname,
                "OpenAlexInstitutionID": iid,
                "Country": rep_inst.get("country_code") or "",
                "Type": rep_inst.get("type") or "",
                "NormName": norm(iname)
            })

    if idx % 100 == 0:
        print(f"fetched {idx}/{len(target_ids)}", flush=True)
    time.sleep(DELAY)

pd.DataFrame(authors).to_csv(OUTPUT_AUTHOR, index=False)
pd.DataFrame(insts).to_csv(OUTPUT_INST, index=False)
print("authors:", len(authors), "->", OUTPUT_AUTHOR)
print("institutions:", len(insts), "->", OUTPUT_INST)
