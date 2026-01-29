"""
Upload-script for the E-LAUTE audio files to the TU-RDM platform.

Usage:
cd scripts
python -m upload_to_RDM.audio-upload

Make sure to set the TESTING_MODE flag to false when ready to upload to the real RDM.

The script will:
1. Copy the audio files from the Google Drive folder to a local folder.
2. Load the form responses from the Google Sheet.
3. Clean the work IDs.
4. Load the ID table.
5. Build the metadata dataframe.
6. Rename the audio files.
7. Extract the audio metadata.
8. Convert the WAV files to MP3.
9. Create the JSON metadata files.
10. Tag the WAV files with the metadata.
11. Upload the new works to the RDM.
12. Update the URL list.
"""

import os
import glob
import json
import re
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import taglib
from dotenv import load_dotenv
from wavinfo import WavInfoReader

from upload_to_RDM.rdm_upload_utils import (
    compare_hashed_files,
    create_related_identifiers,
    get_id_from_api,
    get_records_from_RDM,
    look_up_source_links,
    look_up_source_title,
    make_html_link,
    set_headers,
)

load_dotenv()
pd.set_option("display.max_columns", None)

ROOT_DIR = Path(__file__).resolve().parent

TESTING_MODE = True

MP3_VBR_QUALITY = 2  # LAME VBR ~190-200 kbps (approx. 192 kbps)

RDM_API_URL = None
RDM_TOKEN = None
ELAUTE_COMMUNITY_ID = None
MAPPING_FILE = None
URL_LIST_FILE = None

GOOGLE_DRIVE_DIR = os.getenv("AUDIO_UPLOAD_FORM_DRIVE_DIR_JULIA")
GOOGLE_DRIVE_OWNER = "Julia Jaklin"


def setup_config():
    global RDM_API_URL
    global RDM_TOKEN
    global ELAUTE_COMMUNITY_ID
    global MAPPING_FILE
    global URL_LIST_FILE

    if TESTING_MODE:
        print("🧪 Running in TESTING mode")
        RDM_TOKEN = os.getenv("RDM_TEST_API_TOKEN")
        RDM_API_URL = "https://test.researchdata.tuwien.ac.at/api"
        ELAUTE_COMMUNITY_ID = get_id_from_api(
            "https://test.researchdata.tuwien.ac.at/api/communities/e-laute-test"
        )
        MAPPING_FILE = "work_id_record_id_mapping_TESTING.csv"
        URL_LIST_FILE = "url_list_TESTING.csv"
    else:
        print("🚀 Running in PRODUCTION mode")
        RDM_TOKEN = os.getenv("RDM_API_TOKEN")
        RDM_API_URL = "https://researchdata.tuwien.ac.at/api"
        ELAUTE_COMMUNITY_ID = get_id_from_api(
            "https://researchdata.tuwien.ac.at/api/communities/e-laute"
        )
        MAPPING_FILE = "work_id_record_id_mapping.csv"
        URL_LIST_FILE = "url_list.csv"

    print(f"Using mapping file: {MAPPING_FILE}")
    print(f"Using API URL: {RDM_API_URL}")


def remove_string_from_filenames(root_dir, string_to_remove):
    for dirpath, _dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            if string_to_remove in filename:
                new_filename = filename.replace(string_to_remove, "")
                old_file_path = os.path.join(dirpath, filename)
                new_file_path = os.path.join(dirpath, new_filename)
                os.rename(old_file_path, new_file_path)


def copy_drive_files():
    audio_files_folder_path = (
        f"drive_files/audio_files_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    )
    google_drive_audio_folder = (
        f"{GOOGLE_DRIVE_DIR}/"
        "E-LAUTE audio file and metadata upload (File responses)"
    )

    os.makedirs(audio_files_folder_path, exist_ok=True)
    shutil.copytree(
        google_drive_audio_folder, audio_files_folder_path, dirs_exist_ok=True
    )

    google_sheet_path = (
        f"{GOOGLE_DRIVE_DIR}/"
        "E-LAUTE audio file and metadata upload (Responses).gsheet"
    )
    shutil.copy2(google_sheet_path, audio_files_folder_path)

    folder_name_mapping = {
        "Upload of public release file in .wav format (File responses)": "release",
        "Optional upload of annotated score (screenshot scan) (File responses)": "score",
        "E-LAUTE audio file and metadata upload (Responses).gsheet": "responses.gsheet",
    }

    for old_name, new_name in folder_name_mapping.items():
        old_path = os.path.join(audio_files_folder_path, old_name)
        new_path = os.path.join(audio_files_folder_path, new_name)

        if os.path.exists(old_path):
            os.rename(old_path, new_path)
        else:
            print(f"Folder not found: {old_path}, probably already renamed.")

    return audio_files_folder_path


def load_form_responses(audio_files_folder_path):
    with open(f"{audio_files_folder_path}/responses.gsheet") as f:
        content = f.read()

    sheet_id = json.loads(content).get("doc_id")

    form_responses_df = pd.read_csv(
        f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
    )

    form_responses_df.rename(
        columns={
            "Timestamp": "timestamp",
            "Email address": "email",
            "Work ID": "work_id_forms",
            "Upload of public release file in .wav format": "audio_link",
            "License agreement": "licence",
            "Name of the performer": "performer",
            "Instrument": "instrument",
            "Number of courses": "courses",
            "Tuning": "tuning",
            "Pitch level in Hertz": "hertz",
            "Date of lute making (approx.)": "making_date",
            "Maker of the lute": "maker",
            "Source used for performance": "source_type",
            "URL of score": "source_link",
            "Performance note": "performance_comment",
            "Optional upload of annotated score (screenshot/scan)": "annot_score_link",
            "Date of recording": "date",
            "Place of recording": "place",
            "Recording equipment": "microphone",
            "Audio Interface": "audio_interface",
            "Digital Audio Workstation (Audio-Schnittprogramm)": "daw",
            "Name of the producer": "producer",
            "Recording notes": "recording_comment",
            "UUID": "uuid",
            "RecordingID": "recording_id",
        },
        inplace=True,
    )

    return form_responses_df


def clean_work_ids(form_responses_df):
    split_ids = form_responses_df["work_id_forms"].str.split()
    form_responses_df["work_id"] = split_ids.str[-1]
    form_responses_df["work_id_2"] = split_ids.str[-2]
    form_responses_df["work_id_2"] = form_responses_df["work_id_2"].fillna("")

    special_ids = ["A-Wn_Mus.Hs._18688_n16"]
    for special_id in special_ids:
        form_responses_df.loc[
            form_responses_df["work_id_2"].str.contains(special_id, na=False),
            "work_id",
        ] = (
            special_id + ", " + form_responses_df["work_id"]
        )

    form_responses_df.drop(columns=["work_id_2"], inplace=True)

    return form_responses_df


def load_id_table():
    id_table_path = ROOT_DIR / "tables" / "id_table.xlsx"
    if not id_table_path.exists():
        raise FileNotFoundError(
            "Could not find id_table.xlsx. "
            "Expected in scripts/upload_to_RDM/tables/."
        )
    id_excel_df = pd.read_excel(id_table_path)
    id_table = pd.DataFrame()
    id_table["work_id"] = id_excel_df["IDs"]
    id_table["title"] = id_excel_df["title"]
    id_table["fol_or_p"] = id_excel_df["fol_or_p"]
    return id_table


def load_sources_table():
    sources_table_path = ROOT_DIR / "tables" / "sources_table.xlsx"
    if not sources_table_path.exists():
        raise FileNotFoundError(
            "Could not find sources_table.xlsx. "
            "Expected in scripts/upload_to_RDM/tables/."
        )
    sources_excel_df = pd.read_excel(sources_table_path)
    sources_table = pd.DataFrame()
    sources_table["source_id"] = sources_excel_df["ID"].fillna(
        sources_excel_df["Shelfmark"]
    )
    sources_table["Title"] = sources_excel_df["Title"]
    sources_table["Source_link"] = sources_excel_df["Source_link"].fillna("")
    sources_table["RISM_link"] = sources_excel_df["RISM_link"].fillna("")
    sources_table["VD_16"] = sources_excel_df["VD_16"].fillna("")
    return sources_table


def build_metadata_df(form_responses_df, id_table):
    metadata_df = form_responses_df.copy()

    metadata_df["timestamp"] = pd.to_datetime(
        metadata_df["timestamp"], format="%d/%m/%Y %H:%M:%S", errors="coerce"
    )
    metadata_df["making_date"] = pd.to_numeric(
        metadata_df["making_date"], errors="coerce"
    ).astype("Int64")

    metadata_df[["performer_lastname", "performer_firstname"]] = metadata_df[
        "performer"
    ].str.split(", ", expand=True)

    metadata_df["licence"] = "CC BY-SA 4.0"

    metadata_df[["producer_lastname", "producer_firstname"]] = metadata_df[
        "producer"
    ].str.split(", ", expand=True)

    metadata_df["recording_id"] = (
        metadata_df["work_id"].str.replace(", ", "_")
        + "_"
        + metadata_df["uuid"]
    )

    def extract_before_n(work_id):
        match = re.search(r"(.+?)(?:\s|_n\d+)", work_id)
        return match.group(1) if match else work_id

    metadata_df["source_id"] = metadata_df["work_id"].apply(extract_before_n)

    merged_df = metadata_df.merge(
        id_table[["work_id", "title", "fol_or_p"]], on="work_id", how="left"
    )

    missing_ids_df = merged_df[
        merged_df["title"].isna() | merged_df["fol_or_p"].isna()
    ]
    missing_ids = missing_ids_df["work_id"].tolist()

    for missing_id in missing_ids:
        split_ids = missing_id.split(", ")
        titles = []
        fols = []

        for split_id in split_ids:
            if split_id in id_table["work_id"].tolist():
                title = id_table.loc[
                    id_table["work_id"] == split_id, "title"
                ].values[0]
                fol = id_table.loc[
                    id_table["work_id"] == split_id, "fol_or_p"
                ].values[0]
                titles.append(title)
                fols.append(fol)

        if titles or fols:
            concatenated_title = ", ".join(titles)
            concatenated_fol = ", ".join(fols)
            merged_df.loc[merged_df["work_id"] == missing_id, "title"] = (
                concatenated_title
            )
            merged_df.loc[merged_df["work_id"] == missing_id, "fol_or_p"] = (
                concatenated_fol
            )

    remaining_missing = merged_df[
        merged_df["title"].isna() | merged_df["fol_or_p"].isna()
    ]
    if not remaining_missing.empty:
        print("The following work_ids could not be found in id_table:")
        print(remaining_missing["work_id"].tolist())
    else:
        print("All work_ids were found in id_table.")

    return merged_df


def build_metadata_for_upload(merged_df):
    metadata_for_upload = merged_df.drop(
        columns=["email", "audio_link", "annot_score_link", "work_id_forms"]
    )
    return metadata_for_upload


def rename_audio_files(metadata_for_upload, audio_files_folder_path):
    release_folder = os.path.join(audio_files_folder_path, "release")
    all_audio_files = []
    if os.path.exists(release_folder):
        for filename in os.listdir(release_folder):
            file_path = os.path.join(release_folder, filename)
            if os.path.isfile(file_path):
                all_audio_files.append(file_path)

    for row in metadata_for_upload.itertuples():
        work_id = row.work_id
        fol_or_p = row.fol_or_p
        lastname = row.performer_lastname
        firstname = row.performer_firstname

        matching_file = None
        # special cases because there is an additional space in the work_id in the filename...
        if work_id == "Ger_1533-1_n38":
            matching_file = (
                os.path.dirname(file_path)
                + "/Gerle 1533-1, Adieu mes amours, Ger_ 1533-1_n38 - Christian Velasco.wav"
            )
        if work_id == "Ger_1533-1_n42":
            matching_file = (
                os.path.dirname(file_path)
                + "/Gerle 1533-1, En lombre, Ger_ 1533-1_n42 - Christian Velasco.wav"
            )
        if work_id == "Ger_1533-1_n39":
            matching_file = (
                os.path.dirname(file_path)
                + "/Gerle 1533-1, Mile regres, Ger_ 1533-1_n39 - Christian Velasco.wav"
            )

        if matching_file is None:
            for file_path in all_audio_files:
                filename = os.path.basename(file_path)
                if work_id in filename:
                    matching_file = file_path
                    break

        if matching_file:
            new_filename = f"{work_id}_{fol_or_p}_{lastname}_{firstname}.wav"
            new_filename = re.sub(r'[<>:"/\\|?*]', "", new_filename)
            new_file_path = os.path.join(
                os.path.dirname(matching_file), new_filename
            )
            os.rename(matching_file, new_file_path)
        else:
            print(f"No file found for work_id: {work_id}")


def extract_audio_metadata(folder_path):
    wav_files = glob.glob(os.path.join(folder_path, "*.wav"))
    metadata_list = []

    for file_path in wav_files:
        filename = os.path.basename(file_path)

        metadata = {
            "filename": filename,
            "file_path": file_path,
            "file_size_bytes": os.path.getsize(file_path),
            "audio_format": None,
            "encoding": None,
            "sample_rate": None,
            "sample_rate_khz": None,
            "bits_per_sample": None,
            "channels": None,
            "channel_count": None,
            "channel_description": None,
            "encoding_type": None,
            "duration_seconds": None,
            "total_samples": None,
            "byte_rate": None,
            "block_align": None,
            "technical_spec": None,
            "bext_originator": None,
            "bext_date": None,
            "bext_time": None,
            "bext_time_reference": None,
            "bext_coding_history": None,
            "tags": None,
            "error": None,
        }

        try:
            info = WavInfoReader(file_path, bext_encoding="utf-8")
            audio_format = info.fmt

            metadata["audio_format"] = audio_format.audio_format
            metadata["sample_rate"] = audio_format.sample_rate
            metadata["bits_per_sample"] = audio_format.bits_per_sample
            metadata["channels"] = audio_format.channel_count
            metadata["byte_rate"] = audio_format.byte_rate
            metadata["block_align"] = audio_format.block_align

            if audio_format.audio_format == 1:
                metadata["encoding"] = "Linear PCM"
            elif audio_format.audio_format == 3:
                metadata["encoding"] = "IEEE Float"
            else:
                metadata["encoding"] = (
                    f"Format Code {audio_format.audio_format}"
                )

            if audio_format.channel_count == 1:
                metadata["channel_description"] = "mono"
            elif audio_format.channel_count == 2:
                metadata["channel_description"] = "stereo"
            else:
                metadata["channel_description"] = (
                    f"{audio_format.channel_count}-channel"
                )

            if hasattr(info, "data") and info.data:
                metadata["duration_seconds"] = (
                    info.data.byte_count / audio_format.byte_rate
                )
                metadata["total_samples"] = (
                    info.data.byte_count // audio_format.block_align
                )

            metadata["sample_rate_khz"] = audio_format.sample_rate / 1000
            metadata["bits_per_sample"] = audio_format.bits_per_sample
            metadata["channel_count"] = audio_format.channel_count
            metadata["encoding_type"] = (
                "PCM"
                if audio_format.audio_format == 1
                else (
                    "IEEE Float"
                    if audio_format.audio_format == 3
                    else f"Format {audio_format.audio_format}"
                )
            )
            metadata["technical_spec"] = (
                f"{audio_format.sample_rate / 1000}kHz/"
                f"{audio_format.bits_per_sample}-bit/"
                f"{audio_format.channel_count}ch"
            )

            if hasattr(info, "bext") and info.bext:
                metadata["bext_originator"] = info.bext.originator
                metadata["bext_date"] = info.bext.originator_date
                metadata["bext_time"] = info.bext.originator_time
                if hasattr(info.bext, "time_reference"):
                    metadata["bext_time_reference"] = info.bext.time_reference
                if hasattr(info.bext, "coding_history"):
                    metadata["bext_coding_history"] = info.bext.coding_history

            try:
                f = taglib.File(file_path)
                if f.tags:
                    tags_str = "; ".join(
                        [f"{k}: {', '.join(v)}" for k, v in f.tags.items()]
                    )
                    metadata["tags"] = tags_str
            except Exception:
                pass

        except Exception as e:
            metadata["error"] = str(e)

        metadata_list.append(metadata)

    return pd.DataFrame(metadata_list)


def convert_wav_to_mp3(folder_path):
    ffmpeg_path = shutil.which("ffmpeg")
    if not ffmpeg_path:
        raise RuntimeError(
            "ffmpeg not found in PATH. Please install ffmpeg to enable MP3 "
            "conversion (192 kbps VBR)."
        )

    wav_files = glob.glob(os.path.join(folder_path, "*.wav"))
    if not wav_files:
        print(f"No WAV files found in {folder_path} to convert.")
        return

    for wav_path in wav_files:
        mp3_path = os.path.splitext(wav_path)[0] + ".mp3"
        if os.path.exists(mp3_path):
            if os.path.getmtime(mp3_path) >= os.path.getmtime(wav_path):
                print(f"MP3 already up-to-date: {os.path.basename(mp3_path)}")
                continue

        cmd = [
            ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            wav_path,
            "-codec:a",
            "libmp3lame",
            "-q:a",
            str(MP3_VBR_QUALITY),
            mp3_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(
                f"ffmpeg failed for {wav_path}: {result.stderr.strip()}"
            )


def create_json_metadata(row, audio_metadata_df):
    def safe_get(value, default=""):
        if pd.isna(value):
            return default
        return value

    work_id = row.get("work_id")
    audio_info = None

    for _, audio_row in audio_metadata_df.iterrows():
        if work_id in audio_row.get("filename", ""):
            audio_info = audio_row
            break

    json_metadata = {
        "work_id": safe_get(row.get("work_id")),
        "source_id": safe_get(row.get("source_id")),
        "folio_page": safe_get(row.get("fol_or_p")),
        "title": f"{safe_get(row.get('title'))}",
        "creator": [
            f"Performer: {safe_get(row.get('performer'))}",
        ],
        "subject": [
            "Lute music",
            "16th century music",
            "Historical performance",
            "E-LAUTE project",
            "Audio recording",
        ],
        "description": (
            f"Audio recording of '{safe_get(row.get('title'))}' "
            f"(Work ID: {safe_get(row.get('work_id'))}) performed on "
            f"{safe_get(row.get('instrument'))} by "
            f"{safe_get(row.get('performer'))}. Part of E-LAUTE project."
        ),
        "publisher": "E-LAUTE Project",
        "contributor": [
            f"Performer: {safe_get(row.get('performer'))}",
        ],
        "date": safe_get(row.get("date")),
        "type": "Sound",
        "format": "audio/wav",
        "identifier": safe_get(row.get("work_id")),
        "source": safe_get(row.get("source_id")),
        "rights": safe_get(row.get("licence"), "CC BY-SA 4.0"),
        "created": safe_get(row.get("date")),
        "modified": datetime.now().isoformat(),
        "isPartOf": "E-LAUTE Digital Edition",
        "location_in_source": (
            f"folio/page {safe_get(row.get('fol_or_p'))} "
            f"in source {safe_get(row.get('source_id'))}"
        ),
        "recording_location": safe_get(row.get("place")),
        "recording_date": safe_get(row.get("date")),
        "microphone": safe_get(row.get("microphone")),
        "audio_interface": safe_get(row.get("audio_interface")),
        "daw": safe_get(row.get("daw")),
        "audio_processing_techniques": [
            "crossfade",
            "fade-in",
            "fade-out",
            "equalization",
            "reverberation",
        ],
        "audio_processing_comment": (
            "Multi-take compilation with crossfade editing, amplitude "
            "normalization, parametric equalization, and algorithmic "
            "reverberation. Final render as uncompressed PCM WAV."
        ),
        "daw_plugins": "Valhalla Room",
        "performer": [
            {
                "firstname": safe_get(row.get("performer_firstname")),
                "lastname": safe_get(row.get("performer_lastname")),
                "fullname": safe_get(row.get("performer")),
            }
        ],
        "producer": (
            [
                {
                    "firstname": safe_get(row.get("producer_firstname")),
                    "lastname": safe_get(row.get("producer_lastname")),
                    "fullname": safe_get(row.get("producer")),
                }
            ]
            if pd.notna(row.get("producer"))
            and row.get("producer") not in [None, ""]
            else []
        ),
        "instrument": safe_get(row.get("instrument")),
        "tuning": safe_get(row.get("tuning")),
        "courses": safe_get(row.get("courses")),
        "pitch_hertz": safe_get(row.get("hertz")),
        "lute_maker": safe_get(row.get("maker")),
        "lute_making_date": safe_get(row.get("making_date")),
        "source_type": safe_get(row.get("source_type")),
        "source_link": (
            safe_get(row.get("source_link"))
            if pd.notna(row.get("source_link"))
            else None
        ),
        "performance_comment": safe_get(row.get("performance_comment")),
        "recording_comment": safe_get(row.get("recording_comment")),
    }

    if row.get("producer") not in [None, ""] and pd.notna(row.get("producer")):
        json_metadata["contributor"].append(
            f"Producer: {safe_get(row.get('producer'))}"
        )
        json_metadata["creator"].append(
            f"Producer: {safe_get(row.get('producer'))}"
        )

    if audio_info is not None:
        json_metadata.update(
            {
                "mo:sample_rate": safe_get(audio_info.get("sample_rate")),
                "mo:bitsPerSample": safe_get(audio_info.get("bits_per_sample")),
                "mo:channels": safe_get(audio_info.get("channel_count")),
                "mo:duration": safe_get(audio_info.get("duration_seconds")),
                "mo:encoding": safe_get(audio_info.get("encoding_type")),
                "audio_filename": safe_get(audio_info.get("filename")),
                "audio_file_size_bytes": safe_get(
                    audio_info.get("file_size_bytes")
                ),
                "audio_channel_description": safe_get(
                    audio_info.get("channel_description")
                ),
                "audio_total_samples": safe_get(
                    audio_info.get("total_samples")
                ),
                "audio_byte_rate": safe_get(audio_info.get("byte_rate")),
                "audio_block_align": safe_get(audio_info.get("block_align")),
            }
        )

    return json_metadata


def create_json_files(df, audio_metadata_df, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    for _index, row in df.iterrows():
        metadata = create_json_metadata(row, audio_metadata_df)
        filename = (
            f"{row['work_id']}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
        )
        with open(os.path.join(output_dir, filename), "w") as json_file:
            json.dump(metadata, json_file, indent=4, ensure_ascii=False)


def tag_wav_files_with_metadata(folder_path, metadata_df, sources_table):
    def safe_str(value):
        if pd.isna(value):
            return ""
        return str(value)

    for _index, row in metadata_df.iterrows():
        work_id = row["work_id"]

        audio_file_pattern = os.path.join(folder_path, f"{work_id}_*.wav")
        matching_files = glob.glob(audio_file_pattern)

        if matching_files:
            file_path = matching_files[0]
            filename = os.path.basename(file_path)

            try:
                f = taglib.File(file_path)

                f.tags["PERFORMER:LUTE"] = [safe_str(row["performer"])]
                f.tags["TITLE"] = [safe_str(row["title"])]
                f.tags["COMMENT"] = (
                    "This audio file is a recording of the piece "
                    f"\"{row['title']}\", a 16th century lute music piece "
                    "originally notated in lute tablature, created as part "
                    "of the E-LAUTE project (https://e-laute.info). The "
                    "recording preserves and makes historical lute music "
                    "from the German-speaking regions during 1450-1550 "
                    "accessible. The recording is based on the work with "
                    f"the title \"{row['title']}\" and the id "
                    f"\"{row['work_id']}\" in the e-lautedb. It is found "
                    f"on the page(s) or folio(s) {row['fol_or_p']} in the "
                    f"source \"{look_up_source_title(sources_table, row['source_id'])}\" "
                    f"with the source-id \"{row['source_id']}\"."
                )
                f.tags["DATE"] = [safe_str(row["date"])]
                f.tags["GENRE"] = [
                    "Lute Music",
                    "16th Century Music",
                    "Early Music",
                    "Renaissance Music",
                ]
                f.tags["LICENSE"] = [safe_str(row["licence"])]
                f.tags["ELAUTE_WORK_ID"] = [safe_str(row["work_id"])]
                f.tags["ELAUTE_PERFORMER_FIRSTNAME"] = [
                    safe_str(row["performer_firstname"])
                ]
                f.tags["ELAUTE_PERFORMER_LASTNAME"] = [
                    safe_str(row["performer_lastname"])
                ]
                f.tags["ELAUTE_PRODUCER_FIRSTNAME"] = [
                    safe_str(row["producer_firstname"])
                ]
                f.tags["ELAUTE_PRODUCER_LASTNAME"] = [
                    safe_str(row["producer_lastname"])
                ]
                f.tags["ELAUTE_SOURCE_ID"] = [safe_str(row["source_id"])]
                f.tags["ELAUTE_FOL_OR_P"] = [safe_str(row["fol_or_p"])]
                f.tags["ELAUTE_SOURCE_TYPE"] = [safe_str(row["source_type"])]
                f.tags["ELAUTE_INSTRUMENT"] = [safe_str(row["instrument"])]
                f.tags["ELAUTE_LUTE_MAKER"] = [safe_str(row["maker"])]
                f.tags["ELAUTE_COURSES"] = [safe_str(row["courses"])]
                f.tags["ELAUTE_TUNING"] = [safe_str(row["tuning"])]
                f.tags["ELAUTE_HERTZ"] = [safe_str(row["hertz"])]
                f.tags["ELAUTE_LUTE_MAKING_DATE"] = [
                    safe_str(row["making_date"])
                ]
                f.tags["ELAUTE_PERFORMANCE_COMMENT"] = [
                    safe_str(row["performance_comment"])
                ]
                f.tags["ELAUTE_MICROPHONE"] = [safe_str(row["microphone"])]
                f.tags["ELAUTE_RECORDING_COMMENT"] = [
                    safe_str(row["recording_comment"])
                ]
                f.tags["ELAUTE_AUDIO_INTERFACE"] = [
                    safe_str(row["audio_interface"])
                ]
                f.tags["ELAUTE_DAW"] = [safe_str(row["daw"])]
                f.tags["ELAUTE_PRODUCER"] = [safe_str(row["producer"])]
                f.save()

            except Exception as e:
                print(f"Error tagging {filename}: {e}")
        else:
            print(f"No audio file found for work_id: {work_id}")


def create_description(row, sources_table):
    links_stringified = ""
    links = look_up_source_links(sources_table, row["source_id"])
    for link in links if links else []:
        links_stringified += make_html_link(link) + ", "

    source_id = row["source_id"]
    work_number = row["work_id"].split("_")[-1]
    platform_link = make_html_link(
        "https://edition.onb.ac.at/fedora/objects/"
        f"o:lau.{source_id}/methods/sdef:TEI/get?mode={work_number}"
    )

    part1 = (
        f" <h1>Audio recording of a lute piece from the E-LAUTE project</h1>"
        f"<h2>Overview</h2><p>This dataset contains an audio recording of the "
        f'piece "{row["title"]}", a 16th century lute music piece originally '
        "notated in lute tablature, created as part of the E-LAUTE project "
        '(<a href="https://e-laute.info/">https://e-laute.info/</a>). The '
        "recording preserves and makes historical lute music from the "
        "German-speaking regions during 1450-1550 accessible.</p><p>The "
        f'recording is based on the work with the title "{row["title"]}" '
        f'and the id "{row["work_id"]}" in the e-lautedb. It is found on the '
        f'page(s) or folio(s) {row["fol_or_p"]} in the source '
        f'"{look_up_source_title(sources_table, row["source_id"])}" with the '
        f'source-id "{row["source_id"]}".</p>'
    )

    part4 = (
        "<p>The original source and multiple transcriptions of the work can "
        f"be found on the E-LAUTE platform: {platform_link}.</p>"
    )

    part2 = ""
    if links_stringified not in [None, ""]:
        part2 = f"<p>Links to the source: {links_stringified}.</p>"

    part3 = (
        "<h2>Dataset Contents</h2><p>This dataset includes:</p><ul>"
        "<li><strong>Audio file (wav)</strong>: The audio recording of the lute piece "
        "in .wav format</li>  <li><strong>Audio file (mp3)</strong>: The audio recording of the lute piece "
        "in .mp3 format (192 kbps VBR)</li>  <li><strong>Metadata file</strong>: A metadata "
        "file with detailed information about the recording in .json format"
        "</li></ul><h2>About the E-LAUTE Project</h2><p><strong>E-LAUTE: "
        "Electronic Linked Annotated Unified Tablature Edition - The Lute in "
        "the German-Speaking Area 1450-1550</strong></p><p>The E-LAUTE project "
        "creates innovative digital editions of lute tablatures from the "
        "German-speaking area between 1450 and 1550. This interdisciplinary "
        '"open knowledge platform" combines musicology, music practice, music '
        "informatics, and literary studies to transform traditional editions "
        "into collaborative research spaces.</p><p>For more information, visit "
        'the project website: <a href="https://e-laute.info/">'
        "https://e-laute.info/</a></p>"
    )

    return part1 + part4 + part2 + part3


def fill_out_basic_metadata(row, sources_table):
    metadata = {
        "metadata": {
            "title": f'{row["title"]} ({row["work_id"]}) Audio recording',
            "creators": [
                {
                    "person_or_org": {
                        "family_name": row["performer_lastname"],
                        "given_name": row["performer_firstname"],
                        "name": row["performer"],
                        "type": "personal",
                    },
                    "role": {"id": "other", "title": {"en": "Other"}},
                },
            ],
            "contributors": [
                {
                    "person_or_org": {
                        "family_name": row["performer_lastname"],
                        "given_name": row["performer_firstname"],
                        "name": row["performer"],
                        "type": "personal",
                    },
                    "role": {"id": "other", "title": {"en": "Other"}},
                },
                {
                    "affiliations": [{"id": "04d836q62", "name": "TU Wien"}],
                    "person_or_org": {
                        "family_name": "Jaklin",
                        "given_name": "Julia Maria",
                        "name": "Jaklin, Julia Maria",
                        "type": "personal",
                    },
                    "role": {
                        "id": "contactperson",
                        "title": {"en": "Contact person"},
                    },
                },
            ],
            "description": create_description(row, sources_table),
            "publication_date": datetime.today().strftime("%Y-%m-%d"),
            "dates": [
                {
                    "date": row["date"],
                    "description": "Recording date",
                    "type": {"id": "created", "title": {"en": "Created"}},
                }
            ],
            "publisher": "E-LAUTE",
            "references": [{"reference": "https://e-laute.info/"}],
            "related_identifiers": [],
            "resource_type": {
                "id": "sound",
                "title": {"de": "Audio", "en": "Audio"},
            },
            "rights": [
                {
                    "description": {
                        "en": (
                            "Permits almost any use subject to providing credit "
                            "and license notice. Frequently used for media "
                            "assets and educational materials. The most common "
                            "license for Open Access scientific publications. "
                            "Not recommended for software."
                        )
                    },
                    "icon": "cc-by-sa-icon",
                    "id": "cc-by-sa-4.0",
                    "props": {
                        "scheme": "spdx",
                        "url": (
                            "https://creativecommons.org/licenses/"
                            "by-sa/4.0/legalcode"
                        ),
                    },
                    "title": {
                        "en": (
                            "Creative Commons Attribution Share Alike 4.0 "
                            "International"
                        )
                    },
                }
            ],
            "subjects": [
                {"subject": "16th century"},
                {
                    "id": "http://www.oecd.org/science/inno/38235147.pdf?6.4",
                    "scheme": "FOS",
                    "subject": "Arts (arts, history of arts, performing arts, music)",
                },
                {"subject": "lute music"},
                {"subject": "medieval"},
            ],
        }
    }

    if pd.notna(row["producer"]):
        metadata["metadata"]["contributors"].append(
            {
                "person_or_org": {
                    "family_name": row["producer_lastname"],
                    "given_name": row["producer_firstname"],
                    "name": row["producer"],
                    "type": "personal",
                },
                "role": {"id": "producer", "title": {"en": "Producer"}},
            }
        )

    links_to_source = look_up_source_links(sources_table, row["source_id"])
    if links_to_source:
        if pd.notna(row["source_link"]):
            metadata["metadata"]["related_identifiers"].extend(
                create_related_identifiers(links_to_source)
            )

    return metadata


def upload_new_works_to_RDM(
    metadata_for_upload, audio_files_path, json_metadata_path
):
    record_mapping_data = []
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    h, fh = set_headers(RDM_TOKEN)
    api_url = f"{RDM_API_URL}/records"

    for _index, row in metadata_for_upload.iterrows():
        metadata = fill_out_basic_metadata(row, sources_table)
        r = requests.post(api_url, data=json.dumps(metadata), headers=h)
        assert (
            r.status_code == 201
        ), f"Failed to create record (code: {r.status_code})"
        links = r.json()["links"]
        record_id = r.json()["id"]

        record_mapping_data.append(
            {
                "work_id": row["work_id"],
                "record_id": record_id,
                "created": current_timestamp,
                "updated": current_timestamp,
            }
        )

        audio_files = sorted(
            glob.glob(os.path.join(audio_files_path, f"{row['work_id']}_*.wav"))
        )
        mp3_files = sorted(
            glob.glob(os.path.join(audio_files_path, f"{row['work_id']}_*.mp3"))
        )
        json_files = sorted(
            glob.glob(
                os.path.join(json_metadata_path, f"{row['work_id']}_*.json")
            )
        )

        files = []
        if audio_files:
            files.append(audio_files[0])
        if mp3_files:
            files.append(mp3_files[0])
        if json_files:
            files.append(json_files[0])

        i = 0
        for file_path in files:
            data = json.dumps([{"key": os.path.basename(file_path)}])
            r = requests.post(links["files"], data=data, headers=h)
            assert (
                r.status_code == 201
            ), f"Failed to create file {file_path} (code: {r.status_code})"
            file_links = r.json()["entries"][i]["links"]
            i += 1

            with open(file_path, "rb") as fp:
                r = requests.put(file_links["content"], data=fp, headers=fh)
            assert (
                r.status_code == 200
            ), f"Failed to upload file content {file_path} (code: {r.status_code})"

            r = requests.post(file_links["commit"], headers=h)
            assert (
                r.status_code == 200
            ), f"Failed to commit file {file_path} (code: {r.status_code})"

        r = requests.post(
            f"{RDM_API_URL}/curations",
            headers=h,
            data=json.dumps({"topic": {"record": record_id}}),
        )
        assert (
            r.status_code == 201
        ), f"Failed to create curation for record {record_id} (code: {r.status_code})"

        r = requests.put(
            f"{api_url}/{record_id}/draft/review",
            headers=h,
            data=json.dumps(
                {
                    "receiver": {"community": ELAUTE_COMMUNITY_ID},
                    "type": "community-submission",
                }
            ),
        )
        assert (
            r.status_code == 200
        ), f"Failed to set review for record {record_id} (code: {r.status_code})"

        r = requests.post(
            f"{api_url}/{record_id}/draft/actions/submit-review", headers=h
        )
        assert (
            r.status_code == 202
        ), f"Failed to submit review for record {record_id} (code: {r.status_code})"

    mapping_df = pd.DataFrame(record_mapping_data)
    mapping_df.to_csv(
        MAPPING_FILE, index=False, sep=";", mode="a", header=False
    )

    print(mapping_df.head())


def get_existing_records_by_work_id():
    records_df = get_records_from_RDM(
        RDM_TOKEN, RDM_API_URL, ELAUTE_COMMUNITY_ID
    )
    if records_df is None or records_df.empty:
        print("No records found in RDM.")
        return {}

    records_df = records_df.dropna(subset=["elaute_id"])
    if records_df.empty:
        print("No records with E-LAUTE IDs found in RDM.")
        return {}

    records_df["updated_dt"] = pd.to_datetime(
        records_df["updated"], errors="coerce"
    )
    records_df = records_df.sort_values("updated_dt")
    latest_records = records_df.groupby("elaute_id").tail(1)
    return dict(zip(latest_records["elaute_id"], latest_records["record_id"]))


def update_records_in_RDM(
    metadata_for_upload, audio_files_path, json_metadata_path
):
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    updated_records = []
    existing_records = get_existing_records_by_work_id()

    if not existing_records:
        print("No existing records found. Skipping updates.")
        return

    h, fh = set_headers(RDM_TOKEN)

    def _get_remote_file_info(record_id):
        try:
            r = requests.get(
                f"{RDM_API_URL}/records/{record_id}/files", headers=h
            )
            if r.status_code != 200:
                print(
                    "Failed to fetch file list for record "
                    f"{record_id} (code: {r.status_code})"
                )
                return None
            entries = r.json().get("entries", [])
        except Exception as exc:
            print(f"Failed to fetch file list for record {record_id}: {exc}")
            return None

        remote_info = {}
        for entry in entries:
            key = entry.get("key")
            if not key:
                continue
            remote_info[key] = {
                "checksum": entry.get("checksum"),
                "size": entry.get("size"),
            }
        return remote_info

    def _files_changed(record_id, local_files):
        remote_info = _get_remote_file_info(record_id)
        if remote_info is None:
            print("Could not verify remote files; assuming changes exist.")
            return True

        local_names = {os.path.basename(path) for path in local_files}
        remote_names = set(remote_info.keys())

        if local_names != remote_names:
            missing = local_names - remote_names
            extra = remote_names - local_names
            if missing:
                print(f"Remote missing files: {sorted(missing)}")
            if extra:
                print(f"Remote has extra files: {sorted(extra)}")
            return True

        for path in local_files:
            name = os.path.basename(path)
            remote = remote_info.get(name, {})
            remote_size = remote.get("size")

            if remote_size is not None:
                if not compare_hashed_files(remote_size, os.path.getsize(path)):
                    print(f"Size mismatch for {name}")
                    return True
            else:
                print(f"No checksum/size for {name}; assuming changes exist.")
                return True

        return False

    fields_to_compare = [
        "title",
        "creators",
        "contributors",
        "description",
        "dates",
        "publisher",
        "references",
        "related_identifiers",
        "resource_type",
        "rights",
    ]

    for _index, row in metadata_for_upload.iterrows():
        work_id = row["work_id"]
        record_id = existing_records.get(work_id)
        if not record_id:
            print(f"Work ID {work_id} not found in existing records. Skipping.")
            continue

        print(f"Checking record {record_id} for work_id {work_id}")

        try:
            r = requests.get(f"{RDM_API_URL}/records/{record_id}", headers=h)
            if r.status_code != 200:
                print(
                    f"Failed to fetch record {record_id} (code: {r.status_code})"
                )
                continue

            current_record = r.json()
            current_metadata = current_record.get("metadata", {})

            new_metadata_structure = fill_out_basic_metadata(row, sources_table)
            new_metadata = new_metadata_structure["metadata"]

            metadata_changed = False
            changes_detected = []

            for field in fields_to_compare:
                current_value = current_metadata.get(field)
                new_value = new_metadata.get(field)

                if not compare_hashed_files(current_value, new_value):
                    metadata_changed = True
                    changes_detected.append(field)
                    print(f"  Field '{field}' changed:")
                    print(
                        f"    Current: {json.dumps(current_value, indent=2) if current_value else 'None'}"
                    )
                    print(
                        f"    New: {json.dumps(new_value, indent=2) if new_value else 'None'}"
                    )

            audio_files = sorted(
                glob.glob(os.path.join(audio_files_path, f"{work_id}_*.wav"))
            )
            mp3_files = sorted(
                glob.glob(os.path.join(audio_files_path, f"{work_id}_*.mp3"))
            )
            json_files = sorted(
                glob.glob(os.path.join(json_metadata_path, f"{work_id}_*.json"))
            )
            local_files = []
            if audio_files:
                local_files.extend(audio_files)
            if mp3_files:
                local_files.extend(mp3_files)
            if json_files:
                local_files.extend(json_files)

            files_changed = _files_changed(record_id, local_files)

            if not metadata_changed and not files_changed:
                print(
                    "Warning: no metadata or file changes detected for "
                    f"work_id {work_id}; skipping update."
                )
                continue
            if not metadata_changed and files_changed:
                print(
                    "Metadata unchanged but file changes detected for "
                    f"work_id {work_id}; creating new version."
                )

            print(
                "Metadata changes detected for work_id "
                f"{work_id} in fields: {', '.join(changes_detected)}"
            )

            r = requests.post(
                f"{RDM_API_URL}/records/{record_id}/versions", headers=h
            )
            if r.status_code != 201:
                print(
                    "Failed to create new version for record "
                    f"{record_id} (code: {r.status_code})"
                )
                continue

            new_version_data = r.json()
            new_record_id = new_version_data["id"]
            print(f"Created new version {new_record_id} for work_id {work_id}")

            r = requests.put(
                f"{RDM_API_URL}/records/{new_record_id}/draft",
                data=json.dumps(new_metadata_structure),
                headers=h,
            )
            if r.status_code != 200:
                print(
                    "Failed to update draft "
                    f"{new_record_id} (code: {r.status_code})"
                )
                continue

            if audio_files or mp3_files or json_files:
                r = requests.delete(
                    f"{RDM_API_URL}/records/{new_record_id}/draft/files",
                    headers=h,
                )

                files = local_files

                file_entries = [{"key": os.path.basename(f)} for f in files]
                data = json.dumps(file_entries)
                r = requests.post(
                    f"{RDM_API_URL}/records/{new_record_id}/draft/files",
                    data=data,
                    headers=h,
                )
                if r.status_code != 201:
                    print(
                        "Failed to initialize files for record "
                        f"{new_record_id} (code: {r.status_code})"
                    )
                    continue

                file_responses = r.json()["entries"]

                for i, file_path in enumerate(files):
                    file_links = file_responses[i]["links"]
                    with open(file_path, "rb") as fp:
                        r = requests.put(
                            file_links["content"], data=fp, headers=fh
                        )
                    if r.status_code != 200:
                        print(
                            "Failed to upload file content "
                            f"{file_path} (code: {r.status_code})"
                        )
                        continue

                    r = requests.post(file_links["commit"], headers=h)
                    if r.status_code != 200:
                        print(
                            f"Failed to commit file {file_path} "
                            f"(code: {r.status_code})"
                        )
                        continue

            r = requests.post(
                f"{RDM_API_URL}/records/{new_record_id}/draft/actions/publish",
                headers=h,
            )
            if r.status_code != 202:
                print(
                    f"Failed to publish record {new_record_id} "
                    f"(code: {r.status_code})"
                )
                continue

            updated_records.append(
                {
                    "work_id": work_id,
                    "record_id": new_record_id,
                    "created": current_record.get("created", ""),
                    "updated": current_timestamp,
                }
            )

            print(
                f"Successfully updated record for work_id {work_id}: "
                f"{new_record_id}"
            )

        except Exception as e:
            print(f"Error updating record for work_id {work_id}: {str(e)}")
            continue

    if updated_records:
        updated_df = pd.DataFrame(updated_records)
        updated_df.to_csv(
            MAPPING_FILE, index=False, sep=";", mode="a", header=False
        )
        print(
            f"Updated {len(updated_records)} records and saved to {MAPPING_FILE}"
        )
    else:
        print("No records were updated.")


def process_work_ids(metadata_for_upload, audio_files_path, json_metadata_path):
    existing_records = get_existing_records_by_work_id()
    existing_work_ids = set(existing_records.keys())

    if existing_work_ids:
        print(f"Found {len(existing_work_ids)} existing work_ids in RDM")
    else:
        print("No existing records found in RDM - all records will be created")

    current_work_ids = set(metadata_for_upload["work_id"].tolist())
    print(f"Current batch contains {len(current_work_ids)} work_ids")

    new_work_ids = current_work_ids - existing_work_ids
    existing_work_ids_to_check = current_work_ids & existing_work_ids

    print(f"New work_ids to upload: {len(new_work_ids)}")
    print(
        "Existing work_ids to check for updates: "
        f"{len(existing_work_ids_to_check)}"
    )

    new_records_df = metadata_for_upload[
        metadata_for_upload["work_id"].isin(new_work_ids)
    ]
    existing_records_df = metadata_for_upload[
        metadata_for_upload["work_id"].isin(existing_work_ids_to_check)
    ]

    if not new_records_df.empty:
        print(f"\nCreating {len(new_records_df)} new records...")
        try:
            upload_new_works_to_RDM(
                new_records_df, audio_files_path, json_metadata_path
            )
            print("Successfully created new records")
        except Exception as e:
            print(f"Error creating new records: {str(e)}")
    else:
        print("\nNo new records to create")

    if not existing_records_df.empty:
        print(
            f"\nChecking {len(existing_records_df)} existing records for updates..."
        )
        try:
            update_records_in_RDM(
                existing_records_df, audio_files_path, json_metadata_path
            )
            print("Successfully processed existing records")
        except Exception as e:
            print(f"Error updating existing records: {str(e)}")
    else:
        print("\nNo existing records to check")

    return new_work_ids, existing_work_ids_to_check


def update_url_list():
    h, _fh = set_headers(RDM_TOKEN)

    try:
        record_df = pd.read_csv(
            MAPPING_FILE,
            sep=";",
            quotechar='"',
            quoting=1,
            skipinitialspace=True,
            engine="python",
        )

        if len(record_df) == 0:
            print("No records found in mapping file")
            return

        latest_records = record_df.loc[
            record_df.groupby("work_id")["updated"].idxmax()
        ].copy()

        parent_html_list = []
        self_html_list = []
        title_list = []
        failed_records = []

        for _idx, row in latest_records.iterrows():
            r_id = row["record_id"]

            try:
                r = requests.get(
                    f"{RDM_API_URL}/records/{r_id}", headers=h, timeout=30
                )

                if r.status_code != 200:
                    failed_records.append(r_id)
                    parent_html_list.append("")
                    self_html_list.append("")
                    title_list.append("")
                    continue

                response_json = r.json()
                links = response_json.get("links", {})
                parent_html = links.get("parent_html", "")
                self_html = links.get("self_html", "")

                metadata = response_json.get("metadata", {})
                title = metadata.get("title", "")

                parent_html_list.append(parent_html)
                self_html_list.append(self_html)
                title_list.append(title)

            except requests.exceptions.Timeout:
                failed_records.append(r_id)
                parent_html_list.append("")
                self_html_list.append("")
                title_list.append("")
            except Exception:
                failed_records.append(r_id)
                parent_html_list.append("")
                self_html_list.append("")
                title_list.append("")

        successful = len(parent_html_list) - len(failed_records)
        print(
            "Processed "
            f"{len(latest_records)} records: {successful} successful, "
            f"{len(failed_records)} failed"
        )

        if failed_records:
            print(f"Failed: {failed_records}")

        expected_length = len(latest_records)
        if (
            len(parent_html_list) == expected_length
            and len(self_html_list) == expected_length
            and len(title_list) == expected_length
        ):
            latest_records["title"] = title_list
            latest_records["all_versions_url"] = parent_html_list
            latest_records["current_version_url"] = self_html_list

            column_order = [
                "work_id",
                "record_id",
                "title",
                "created",
                "updated",
                "all_versions_url",
                "current_version_url",
            ]
            latest_records = latest_records[column_order]

            latest_records.to_csv(
                URL_LIST_FILE, index=False, sep=";", quoting=1
            )
            print(f"Results saved to {URL_LIST_FILE}")
        else:
            print("Length mismatch - results not saved")

    except FileNotFoundError:
        print(f"Mapping file not found: {MAPPING_FILE}")
    except Exception as e:
        print(f"Error: {str(e)}")


def main():
    setup_config()

    audio_files_folder_path = copy_drive_files()

    form_responses_df = load_form_responses(audio_files_folder_path)
    form_responses_df = clean_work_ids(form_responses_df)

    id_table = load_id_table()
    global sources_table
    sources_table = load_sources_table()

    merged_df = build_metadata_df(form_responses_df, id_table)
    metadata_for_upload = build_metadata_for_upload(merged_df)

    if TESTING_MODE:
        metadata_for_upload = metadata_for_upload.head(1)
        print(
            "TESTING mode: limiting upload to "
            f"{len(metadata_for_upload)} work_id"
        )

    rename_audio_files(metadata_for_upload, audio_files_folder_path)

    release_folder = os.path.join(audio_files_folder_path, "release")
    audio_metadata_df = extract_audio_metadata(release_folder)

    json_folder_path = "json_metadata_files/" + datetime.now().strftime(
        "%Y%m%d%H%M%S"
    )
    create_json_files(metadata_for_upload, audio_metadata_df, json_folder_path)

    tag_wav_files_with_metadata(
        release_folder, metadata_for_upload, sources_table
    )

    convert_wav_to_mp3(release_folder)

    audio_files_path = audio_files_folder_path + "/release"
    json_metadata_path = json_folder_path

    new_ids, existing_ids = process_work_ids(
        metadata_for_upload, audio_files_path, json_metadata_path
    )

    print("\nProcessing complete:")
    print(f"Total work_ids processed: {len(metadata_for_upload)}")
    print(f"New records created: {len(new_ids)}")
    print(f"Existing records checked: {len(existing_ids)}")

    update_url_list()


if __name__ == "__main__":
    main()
