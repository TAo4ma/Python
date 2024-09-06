import os
import shutil
import time
import random
from concurrent.futures import ThreadPoolExecutor
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class FileProcessor:
    def __init__(self, tiff_folder, pscan_in_folder, pscan_out_folder, pdf_folder, error_folder, debug_file_count=0):
        self.tiff_folder = tiff_folder
        self.pscan_in_folder = pscan_in_folder
        self.pscan_out_folder = pscan_out_folder
        self.pdf_folder = pdf_folder
        self.error_folder = error_folder
        self.debug_file_count = debug_file_count
        self.error_log = []
        self.results = []
        self.file_events = {}
    
    def process_files(self, continue_processing):
        tif_files = [f for f in os.listdir(self.tiff_folder) if f.endswith('.tif')]
        
        if self.debug_file_count > 0:
            tif_files = tif_files[:self.debug_file_count]
        
        processed_files = [os.path.splitext(f)[0] for f in os.listdir(self.pdf_folder) if f.endswith('.pdf')]
        
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(self.process_file, tif_file, processed_files, continue_processing) for tif_file in tif_files]
            for future in futures:
                future.result()
        
        self.write_log()
        print("すべての並列処理が完了しました。")
    
    def process_file(self, tif_file, processed_files, continue_processing):
        if continue_processing and os.path.splitext(tif_file)[0] in processed_files:
            self.results.append(f"File {tif_file} skipped (already processed)")
            return
        
        file_event = FileSystemEventHandler()
        self.file_events[tif_file] = file_event
        
        try:
            destination_file_path = os.path.join(self.pscan_in_folder, tif_file)
            shutil.copy(os.path.join(self.tiff_folder, tif_file), destination_file_path)
            
            time.sleep(random.randint(1, 5))
            
            # ファイル生成イベントを待機
            while not file_event.is_set():
                time.sleep(1)
            
            renamed_file_path = os.path.join(self.pscan_out_folder, tif_file)
            final_file_path = os.path.join(self.pdf_folder, tif_file)
            shutil.move(renamed_file_path, final_file_path)
            
            self.results.append(f"File {tif_file} processed successfully")
        except Exception as e:
            error_file_path = os.path.join(self.error_folder, tif_file)
            shutil.move(os.path.join(self.tiff_folder, tif_file), error_file_path)
            self.error_log.append(f"File {tif_file} failed: {str(e)}")
    
    def write_log(self):
        log_file = os.path.join(self.tiff_folder, "logfile.txt")
        with open(log_file, 'w') as f:
            for result in self.results:
                f.write(result + '\n')
            for error in self.error_log:
                f.write(error + '\n')
    
    def initialize(self):
        # 指定されたフォルダ内のすべてのファイルとディレクトリを削除
        for folder in [self.pscan_in_folder, self.pscan_out_folder, self.pdf_folder, self.error_folder]:
            for filename in os.listdir(folder):
                file_path = os.path.join(folder, filename)
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
        # ログファイルが存在する場合は削除
        log_file = os.path.join(self.tiff_folder, "logfile.txt")
        if os.path.exists(log_file):
            os.remove(log_file)
        print("初期化が完了しました。")

class FileCreatedHandler(FileSystemEventHandler):
    def __init__(self, file_events):
        self.file_events = file_events
    
    def on_created(self, event):
        # 新しいファイルが作成されたときにイベントを設定
        if event.src_path in self.file_events:
            self.file_events[event.src_path].set()

def main():
    common_path = "Z:\\管理課\\管理課共有資料\\ArcSuite\\eValue図面検索データ_240310\\"
    tiff_folders = [
        "図面検索【最新版】￥図面",
        "図面検索【最新版】￥通知書",
        "図面検索【最新版】￥個装",
        "図面検索【旧版】￥図面(旧)",
        "図面検索【旧版】￥個装"
    ]
    
    pscan_in_folder = "\\\\10.23.2.28\\HGPscanServPlus5\\Job02_OCR\\OCR_IN"
    pscan_out_folder = "\\\\10.23.2.28\\HGPscanServPlus5\\Job02_OCR\\OCR_OUT"
    
    for tiff_folder in tiff_folders:
        tiff_folder_path = os.path.join(common_path, tiff_folder)
        pdf_folder_base = os.path.join(tiff_folder_path, "PDF")
        pdf_err_folder_base = os.path.join(tiff_folder_path, "ERR")
        
        file_processor = FileProcessor(tiff_folder_path, pscan_in_folder, pscan_out_folder, pdf_folder_base, pdf_err_folder_base)
        
        if initialize:
            # 初期化フラグがTrueの場合、初期化メソッドを呼び出す
            file_processor.initialize()
        else:
            # 初期化フラグがFalseの場合、ファイル処理を開始
            file_processor.process_files(continue_processing)

if __name__ == "__main__":
    initialize = False  # 初期化フラグ
    continue_processing = True  # 継続フラグ
    main()