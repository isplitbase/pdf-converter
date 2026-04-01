import os
import re
import json
import subprocess
import tempfile
import socket
import urllib.request
import urllib.parse
from typing import List, Tuple, Optional, Dict, Any

from google.cloud import storage
from pdf2image import convert_from_path

# 追加: MySQL疎通チェック用（軽量・純Python）
try:
    import pymysql
except Exception:
    pymysql = None


# =============================
# Env config (Cloud Run Jobs)
# =============================
INPUT_GS = os.environ.get("INPUT_GS", "").strip()
OUTPUT_GS = os.environ.get("OUTPUT_GS", "").strip()
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "converted").strip()

TARGET_W = int(os.environ.get("TARGET_W", "3307"))
TARGET_H = int(os.environ.get("TARGET_H", "4677"))
USE_CROPBOX = os.environ.get("USE_CROPBOX", "true").lower() in ("1", "true", "yes", "y")
THREAD_COUNT = int(os.environ.get("THREAD_COUNT", "1"))
GS_DPI = int(os.environ.get("GS_DPI", "400"))
NUMBER_FORMAT = os.environ.get("NUMBER_FORMAT", "03d")

MYSQL_CHECK = os.environ.get("MYSQL_CHECK", "true").lower() in ("1", "true", "yes", "y")
MYSQL_HOST = os.environ.get("MYSQL_HOST", "10.146.0.2").strip()
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_USER = os.environ.get("MYSQL_USER", "IsplitAdmin").strip()
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "")
MYSQL_DB = os.environ.get("MYSQL_DB", "").strip()
MYSQL_CONNECT_TIMEOUT = int(os.environ.get("MYSQL_CONNECT_TIMEOUT", "3"))

UPLOAD_FILE_KEYS_RAW = os.environ.get("UPLOAD_FILE_KEYS", "").strip()

AI_CASE_ID = os.environ.get("AI_CASE_ID", "").strip()
ANALYGENT_PORT = int(os.environ.get("ANALYGENT_PORT", "8056"))

# デプロイ確認用（ログに出す）
CODE_VERSION = "2026-01-30-main-patched-v2"


# =============================
# Helpers
# =============================
def parse_gs_uri(gs_uri: str) -> Tuple[str, str]:
    m = re.match(r"^gs://([^/]+)/(.+)$", gs_uri)
    if not m:
        raise ValueError("gs://bucket/object の形式で指定してください")
    return m.group(1), m.group(2)


def split_gs_uri_allow_empty_object(gs_uri: str) -> Tuple[str, str]:
    """
    OUTPUT_GS で gs://bucket/dir/ のように末尾 / を許容したい場合に使う。
    - gs://bucket/dir/  -> bucket, dir/
    - gs://bucket/prefix- -> bucket, prefix-
    """
    m = re.match(r"^gs://([^/]+)/(.*)$", gs_uri)
    if not m:
        raise ValueError("OUTPUT_GS は gs://bucket/... の形式で指定してください")
    return m.group(1), m.group(2)


def input_pdf_basename_no_ext(obj_path: str) -> str:
    name = obj_path.rsplit("/", 1)[-1]
    if name.lower().endswith(".pdf"):
        return name[:-4]
    if "." in name:
        return name.rsplit(".", 1)[0]
    return name


def format_index(i: int) -> str:
    try:
        return format(i, NUMBER_FORMAT)
    except Exception:
        return f"{i:03d}"


def run_ghostscript_normalize(in_pdf: str, out_pdf: str) -> None:
    cmd = [
        "gs",
        "-sDEVICE=pdfwrite",
        "-dCompatibilityLevel=1.7",
        "-dNOPAUSE",
        "-dBATCH",
        "-dSAFER",

        "-dFIXEDMEDIA",
        "-sPAPERSIZE=a4",
        "-dPDFFitPage",

        "-dAutoRotatePages=/None",
        "-dDetectDuplicateImages=true",

        "-dDownsampleColorImages=true",
        "-dColorImageDownsampleType=/Average",
        f"-dColorImageResolution={GS_DPI}",

        "-dDownsampleGrayImages=true",
        "-dGrayImageDownsampleType=/Average",
        f"-dGrayImageResolution={GS_DPI}",

        "-dDownsampleMonoImages=true",
        "-dMonoImageDownsampleType=/Subsample",
        f"-dMonoImageResolution={GS_DPI}",

        f"-sOutputFile={out_pdf}",
        in_pdf,
    ]

    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if r.returncode != 0:
        raise RuntimeError(
            f"Ghostscript failed (code={r.returncode}). stderr:\n{r.stderr[-2000:]}"
        )


def convert_pdf_to_pngs(
    fixed_pdf: str,
    out_dir: str,
    w: int,
    h: int,
    use_cropbox: bool,
    threads: int
) -> List[str]:
    return convert_from_path(
        fixed_pdf,
        size=(w, h),
        fmt="png",
        output_folder=out_dir,
        output_file="page",
        paths_only=True,
        thread_count=threads,
        use_cropbox=use_cropbox,
        use_pdftocairo=True,
        strict=False,
        single_file=False
    )


def resolve_output_target(
    input_bucket: str,
    input_obj: str,
    output_gs: str
) -> Tuple[str, str]:
    """
    戻り値: (out_bucket, out_prefix)
    out_prefix は「オブジェクト名の途中まで（prefix-）」を返す。
    """
    base = input_pdf_basename_no_ext(input_obj)

    if not output_gs:
        out_dir = OUTPUT_DIR.strip().strip("/")
        if out_dir:
            return input_bucket, f"{out_dir}/{base}-"
        return input_bucket, f"{base}-"

    out_bucket, out_obj = split_gs_uri_allow_empty_object(output_gs)

    if out_obj.endswith("/"):
        return out_bucket, f"{out_obj}{base}-"

    if not out_obj:
        return out_bucket, f"{base}-"

    return out_bucket, out_obj


def log_json(payload: dict) -> None:
    # Cloud Run Jobs ではログ遅延/欠落を避けるため flush する
    print(json.dumps(payload, ensure_ascii=False), flush=True)


def post_progress(message: str) -> None:
    """進捗メッセージを analygent サーバへ POST する（失敗しても処理は継続）"""
    global AI_CASE_ID, ANALYGENT_PORT  # app.py から conv.ANALYGENT_PORT = req.port でセットされる
    if not AI_CASE_ID:
        return
    url = f"https://corp.analygent.com:{ANALYGENT_PORT}/sapis/set_upload_files_status_bvvu0xwac2afl7ubhkqj.php"
    content = json.dumps({"message": message}, ensure_ascii=False)
    data = urllib.parse.urlencode({
        "pqlxf4xct4jdsphk8kgc": "uptprogress",
        "ai_case_id": AI_CASE_ID,
        "f1htbrtxki4x7s4s0xqj": content,
    }).encode("utf-8")
    try:
        req = urllib.request.Request(url, data=data)
        with urllib.request.urlopen(req, timeout=5):
            pass
        log_json({"ok": True, "stage": "post_progress", "message": message})
    except Exception as e:
        log_json({"ok": False, "stage": "post_progress_error", "message": message, "error": str(e)})


# =============================
# ai_case_id & img_urls update helpers (修正)
# =============================
def parse_input_gs_list(raw: str) -> List[str]:
    """INPUT_GS を単一 or 複数指定で受け取る。
    - JSON配列:  ["gs://...","gs://..."]
    - 区切り文字: カンマ / 改行 / 空白
    """
    s = (raw or "").strip()
    if not s:
        return []
    if s.startswith("["):
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return [str(x).strip() for x in arr if str(x).strip()]
        except Exception:
            pass
    parts = re.split(r"[\s,]+", s)
    return [p.strip() for p in parts if p.strip()]


def parse_upload_file_keys(raw: str) -> List[str]:
    s = (raw or "").strip()
    if not s:
        return []
    if s.startswith("["):
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return [str(x).strip() for x in arr if str(x).strip()]
        except Exception:
            pass
    parts = re.split(r"[\s,]+", s)
    return [p.strip() for p in parts if p.strip()]


def derive_ai_case_id_from_input_obj(obj_path: str) -> str:
    name = obj_path.rsplit("/", 1)[-1]
    m = re.match(r"^(\d+)[-_]", name)
    if not m:
        raise ValueError(f"ai_case_id をファイル名から抽出できません: {name} (例: 10-xxx.pdf)")
    return m.group(1)


def mysql_connect(database: Optional[str]):
    if pymysql is None:
        raise RuntimeError("pymysql_not_installed")
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=(database or None),  # Noneならスキーマ未指定で接続
        connect_timeout=MYSQL_CONNECT_TIMEOUT,
        read_timeout=MYSQL_CONNECT_TIMEOUT,
        write_timeout=MYSQL_CONNECT_TIMEOUT,
        charset="utf8mb4",
        autocommit=True,
    )


def detect_db_with_ai_case(conn) -> Optional[str]:
    """ai_case テーブルが存在するDBを自動検出する。"""
    # 1) information_schema
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_schema
                FROM information_schema.tables
                WHERE table_name='ai_case'
                  AND table_schema NOT IN ('information_schema','mysql','performance_schema','sys')
                ORDER BY table_schema
            """)
            rows = cur.fetchall()
        schemas = [r[0] for r in rows] if rows else []
        if len(schemas) == 1:
            return schemas[0]
        if len(schemas) > 1:
            raise RuntimeError(f"MYSQL_DB is empty and ai_case exists in multiple schemas: {schemas}")
    except Exception:
        pass

    # 2) fallback
    found = []
    with conn.cursor() as cur:
        cur.execute("SHOW DATABASES")
        dbs = [r[0] for r in cur.fetchall()]

    for db in dbs:
        if db in ("information_schema", "mysql", "performance_schema", "sys"):
            continue
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT 1 FROM `{db}`.`ai_case` LIMIT 1")
                _ = cur.fetchone()
            found.append(db)
        except Exception:
            continue

    if len(found) == 1:
        return found[0]
    if len(found) == 0:
        return None
    raise RuntimeError(f"MYSQL_DB is empty and ai_case exists in multiple visible schemas: {found}")


def mysql_update_ai_case_img_urls(ai_case_id: str, img_urls_joined: str) -> Tuple[bool, str]:
    """ai_case.img_urls を更新する。MYSQL_DB未指定なら自動検出して更新する。"""
    if not MYSQL_USER:
        return False, "skip_update: MYSQL_USER is empty"
    if pymysql is None:
        return False, "pymysql_not_installed"

    try:
        if MYSQL_DB:
            db = MYSQL_DB
        else:
            conn0 = mysql_connect(None)
            try:
                db = detect_db_with_ai_case(conn0)
            finally:
                conn0.close()
            if not db:
                return False, "skip_update: MYSQL_DB is empty and ai_case table not found"

        conn = mysql_connect(db)
        try:
            with conn.cursor() as cur:
                sql = f"UPDATE `{db}`.`ai_case` SET img_urls=%s WHERE ai_case_id=%s"
                cur.execute(sql, (img_urls_joined, ai_case_id))
                affected = cur.rowcount
        finally:
            conn.close()

        return True, f"updated: db={db}, affected_rows={affected}"
    except Exception as e:
        return False, f"update_failed: {e}"


# =============================
# MySQL connectivity check (追加)
# =============================
def tcp_probe(host: str, port: int, timeout_sec: int) -> Tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout_sec):
            return True, "tcp_ok"
    except Exception as e:
        return False, f"tcp_ng: {e}"


def mysql_probe(
    host: str,
    port: int,
    user: str,
    password: str,
    db: str,
    timeout_sec: int
) -> Tuple[bool, str]:
    if not user:
        return False, "skip_mysql_login: MYSQL_USER is empty"
    if pymysql is None:
        return False, "pymysql_not_installed"

    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=db if db else None,
            connect_timeout=timeout_sec,
            read_timeout=timeout_sec,
            write_timeout=timeout_sec,
            charset="utf8mb4",
            autocommit=True,
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                row = cur.fetchone()
        finally:
            conn.close()

        return True, f"mysql_ok: SELECT 1 => {row}"
    except Exception as e:
        return False, f"mysql_ng: {e}"


# =============================
# Main (Job entrypoint)
# =============================
def main() -> Dict[str, Any]:
    upload_file_keys = parse_upload_file_keys(UPLOAD_FILE_KEYS_RAW)

    # 追加: MySQL疎通チェック（最初に実施）
    if MYSQL_CHECK:
        tcp_ok, tcp_msg = tcp_probe(MYSQL_HOST, MYSQL_PORT, MYSQL_CONNECT_TIMEOUT)
        mysql_ok, mysql_msg = mysql_probe(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            db=MYSQL_DB,
            timeout_sec=MYSQL_CONNECT_TIMEOUT,
        )

        log_json({
            "ok": tcp_ok and (mysql_ok or MYSQL_USER == ""),
            "stage": "mysql_check",
            "mysql_host": MYSQL_HOST,
            "mysql_port": MYSQL_PORT,
            "tcp": {"ok": tcp_ok, "message": tcp_msg},
            "mysql": {"ok": mysql_ok, "message": mysql_msg},
            "note": "MYSQL_USER未設定の場合はTCP疎通のみチェックします",
        })

    inputs = parse_input_gs_list(INPUT_GS)
    if not inputs:
        raise ValueError("環境変数 INPUT_GS は必須です（単一: gs://... または複数: JSON配列/カンマ区切り）")

    if len(inputs) > 1 and OUTPUT_GS and not OUTPUT_GS.endswith("/"):
        raise ValueError("複数INPUT_GSの場合は OUTPUT_GS を 'gs://bucket/dir/' のように末尾/で指定してください（自動で <inputbasename>-NNN.png を作ります）")

    client = storage.Client()

    # ai_case_id を入力ファイル名の先頭から抽出（全入力で同じであることを期待）
    ai_case_ids = []
    for gs_uri in inputs:
        _b, _o = parse_gs_uri(gs_uri)
        ai_case_ids.append(derive_ai_case_id_from_input_obj(_o))
    ai_case_id = ai_case_ids[0]
    if any(x != ai_case_id for x in ai_case_ids):
        raise ValueError(f"入力ファイルの ai_case_id が一致しません: {ai_case_ids}")

    all_uploaded_images: List[str] = []

    log_json({
        "ok": True,
        "stage": "start",
        "code_version": CODE_VERSION,
        "inputs": inputs,
        "upload_file_keys": upload_file_keys,
        "output_gs": OUTPUT_GS or "(auto)",
        "target": [TARGET_W, TARGET_H],
        "use_cropbox": USE_CROPBOX,
        "thread_count": THREAD_COUNT,
        "gs_dpi": GS_DPI,
        "ai_case_id": ai_case_id,
        "mysql_db": MYSQL_DB or "(auto-detect)"
    })

    for idx_input, one_input in enumerate(inputs, start=1):
        post_progress(f"{idx_input}番目PDFを画像に変換中")

        in_bucket, in_obj = parse_gs_uri(one_input)
        out_bucket, out_prefix = resolve_output_target(in_bucket, in_obj, OUTPUT_GS)

        bucket_in = client.bucket(in_bucket)
        bucket_out = client.bucket(out_bucket)

        log_json({
            "ok": True,
            "stage": "convert_one",
            "input_gs": one_input,
            "resolved_output": f"gs://{out_bucket}/{out_prefix}<NNN>.png",
        })

        with tempfile.TemporaryDirectory() as td:
            in_pdf = os.path.join(td, "input.pdf")
            fixed_pdf = os.path.join(td, "fixed.pdf")
            out_dir = os.path.join(td, "out")
            os.makedirs(out_dir, exist_ok=True)

            # 1) download PDF
            bucket_in.blob(in_obj).download_to_filename(in_pdf)

            # 2) normalize PDF
            run_ghostscript_normalize(in_pdf, fixed_pdf)

            # 3) render PNGs
            png_paths = convert_pdf_to_pngs(
                fixed_pdf=fixed_pdf,
                out_dir=out_dir,
                w=TARGET_W,
                h=TARGET_H,
                use_cropbox=USE_CROPBOX,
                threads=THREAD_COUNT
            )

            # 4) upload to GCS
            uploaded = []
            for i, path in enumerate(png_paths, start=1):
                idx = format_index(i)
                out_obj = f"{out_prefix}{idx}.png"
                bucket_out.blob(out_obj).upload_from_filename(
                    path,
                    content_type="image/png"
                )
                uploaded.append(f"gs://{out_bucket}/{out_obj}")

        all_uploaded_images.extend(uploaded)

    log_json({
        "ok": True,
        "stage": "pre_mysql_update",
        "ai_case_id": ai_case_id,
        "img_urls_count": len(all_uploaded_images),
        "mysql_db_env": MYSQL_DB,
    })

    # 画像URLを「|,|」区切りで ai_case.img_urls に保存
    img_urls_joined = "|,|".join(all_uploaded_images)

    post_progress("読取完了まで")

    upd_ok, upd_msg = mysql_update_ai_case_img_urls(ai_case_id=ai_case_id, img_urls_joined=img_urls_joined)

    result = {
        "ok": True,
        "stage": "done",
        "ai_case_id": ai_case_id,
        "img_urls_delimiter": "|,|",
        "img_urls_count": len(all_uploaded_images),
        "mysql_update": {"ok": upd_ok, "message": upd_msg},
        "images": all_uploaded_images,
    }

    log_json(result)
    return result


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_json({"ok": False, "error": str(e)})
        raise