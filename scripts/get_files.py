import os
import shutil


PATH_TO_FOLDERS = "/Users/jaklin/Desktop/E-LAUTE/e-laute"


relevant_repos = [
    # "A-Wn_Cod._9704",
    # # "A-Wn_Mus.Hs._18688",
    # # "A-Wn_Mus.Hs._18827",
    # "A-Wn_Mus.Hs._41950",
    # # "CH-Bu_Ms_F_IX_56",
    # # "D-B_Mus.ms._40588",
    # # "D-KA_Don_Mus._Autogr._1",
    # # "D-LEm_I._191",
    # # "D-Mbs_Mus.ms._1511b",
    # # "D-Mbs_Mus.ms._1511c",
    # # "D-Mbs_Mus.ms._1511d",
    # "D-Mbs_Mus.ms._1512",
    # # "D-Mbs_Mus.ms._266",
    # # "D-Mbs_Mus.ms._267",
    # # "D-Mbs_Mus.ms._268",
    # # "D-Mbs_Mus.ms._269",
    # # "D-Mbs_Mus.ms._270",
    # # "D-Mbs_Mus.ms._271",
    # # "D-Mbs_Mus.ms._2987",
    # # "Gerle_Musica_Teusch_1532",
    # # "Gerle_Tabulatur_1533",
    # "Judenkunig_Underweisung_1523-2",
    "Judenkunig_Utilis-compendiaria-introductio_151",
    # # "Newsidler_Ein_Newgeordent_1536_v1",
    # # "Newsidler_Ein_Newgeordent_1536_v2",
    # "PL-KJ_40154",
    # "PL-WRk_352",
    # "Schlick_Tabulaturen_1512",
    # "Sotheby_tablature",
]


def get_files(path_to_folders):
    """
    Find all MEI files in subfolders of relevant repositories and copy them to a files folder.
    """
    # Create MEI directory if it doesn't exist
    mei_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "MEI-files"
    )
    os.makedirs(mei_dir, exist_ok=True)

    # Process each relevant repository
    for repo in relevant_repos:
        repo_path = os.path.join(path_to_folders, repo)
        if not os.path.exists(repo_path):
            print(f"Warning: Repository {repo} not found at {repo_path}")
            continue

        # Walk through subdirectories of this repository
        for root, dirs, files in os.walk(repo_path):
            for file in files:
                if file.endswith(".mei"):
                    source_file = os.path.join(root, file)
                    # Keep original filename
                    destination_file = os.path.join(mei_dir, file)

                    # Copy the file
                    try:
                        shutil.copy2(source_file, destination_file)
                        print(f"Copied {source_file} to {destination_file}")
                    except Exception as e:
                        print(f"Error copying {source_file}: {str(e)}")


def main():
    # Collect all MEI files from subfolders of relevant repositories
    get_files(PATH_TO_FOLDERS)


if __name__ == "__main__":
    main()
