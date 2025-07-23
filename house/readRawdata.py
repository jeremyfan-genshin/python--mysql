import os
import pyarrow  
import pandas

'''
以下將 data_csv資料夾內的 .csv檔案，讀出後忽略第一列的中文欄位資料，另存新檔為.parquet。
'''
import os
import pandas as pd
import pyarrow  # 確保已安裝 pyarrow

def csvToParquet():
    path = os.path.dirname(__file__)
    path = os.path.join(path, 'data_csv')

    for filename in os.listdir(path):
        if filename.endswith('.csv'):
            fullpath = os.path.join(path, filename)
            print(f"找到 .csv 的檔案: {fullpath}")

            try:
                # 嘗試讀取 CSV（跳過第一列）
                df = pd.read_csv(fullpath, skiprows=1)
            except Exception as e:
                print(f"[錯誤] 無法讀取 CSV：{fullpath}\n原因：{e}")
                continue  # 跳過此檔案，繼續處理下一個

            try:
                # 構造 .parquet 檔案路徑
                parquet_filename = os.path.splitext(filename)[0] + '.parquet'
                parquet_path = os.path.join(path, parquet_filename)

                # 嘗試儲存為 Parquet
                df.to_parquet(parquet_path, engine="pyarrow", index=False)
                print(f"轉換完成 ➜ {parquet_path}")
            except Exception as e:
                print(f"[錯誤] 無法轉換為 Parquet：{fullpath}\n原因：{e}")
                continue

    print(f"轉換程序結束，來源資料夾：{path}")


'''
讀取csv檔，轉存成parquet檔
'''
import os
import pandas as pd
import pyarrow

def convert_single_csv_to_parquet(csv_path, skiprows=1, output_path=None):
    """
    將單一 CSV 檔轉換為 Parquet 檔。

    參數:
        csv_path (str): CSV 檔案的完整路徑
        skiprows (int): 要跳過的資料列數（預設為 1）
        output_path (str or None): 輸出的 Parquet 路徑。若為 None，則與 CSV 同資料夾同檔名。

    回傳:
        str or None: 成功時回傳 Parquet 檔路徑；失敗時回傳 None
    """
    try:
        # 讀取 CSV（跳過第一列）
        df = pd.read_csv(csv_path, skiprows=skiprows)
    except Exception as e:
        print(f"[錯誤] 讀取 CSV 失敗：{csv_path}\n原因：{e}")
        return None

    try:
        # 建立輸出路徑（若未指定）
        if output_path is None:
            base, _ = os.path.splitext(csv_path)
            output_path = base + '.parquet'

        # 儲存為 Parquet
        df.to_parquet(output_path, engine="pyarrow", index=False)
        print(f"✅ 轉換完成 ➜ {output_path}")
        return output_path
    except Exception as e:
        print(f"[錯誤] 無法寫入 Parquet：{output_path}\n原因：{e}")
        return None


'''
讀取parquet檔，進行日期的修正。
'''
import pandas as pd
import re
import traceback

def try_fix_minguo_with_letter(val):
    if pd.isna(val):
        return val
    s = str(val).lower()
    char_map = {
        'a': '1', 'b': '2', 'c': '3', 'd': '4',
        'e': '5', 'f': '6', 'g': '7', 'h': '8',
        'i': '9', 'o': '0', 'l': '1'
    }
    s_fixed = ''.join(char_map.get(c, c) for c in s)
    if re.fullmatch(r'\d{7}', s_fixed):
        try:
            year = int(s_fixed[:3]) + 1911
            month = int(s_fixed[3:5])
            day = int(s_fixed[5:7])
            return f"{year:04d}-{month:02d}-{day:02d}"
        except:
            return None
    return None

def fix_date_columns_custom(parquet_path, fill_strategy='mode', save=True, overwrite=False):
    try:
        df = pd.read_parquet(parquet_path)
    except Exception as e:
        print(f"❌ 讀取 parquet 檔案失敗: {parquet_path}")
        print("錯誤訊息:", e)
        print(traceback.format_exc())
        return None

    date_cols = [col for col in df.columns if any(k in col.lower() for k in ['date', 'day', 'year'])]
    if not date_cols:
        print("❌ 沒找到任何包含 'date', 'day', 'year' 的欄位。")
        return df

    print(f"🗂️ 找到日期欄位: {date_cols}")

    for col in date_cols:
        try:
            print(f"\n🔧 處理欄位: {col}")
            original = df[col].astype(str).str.strip()

            # 移除尾巴 .0（避免 float 造成誤差）
            original = original.str.replace(r'\.0$', '', regex=True)

            # 修正含字母的民國日期
            fix_mask = original.str.contains(r'[a-zA-Z]', na=False)
            original.loc[fix_mask] = original[fix_mask].apply(try_fix_minguo_with_letter)

            # 排除超長數字字串（可能錯誤），注意處理 None/NaN 防止錯誤
            mask = original.str.match(r'^\d{10,}$')
            mask = mask.fillna(False)  # 這裡防止 None 造成 ~ 錯誤
            original = original.where(~mask)

            # 民國年轉西元格式
            original = original.str.replace(
                r'^(\d{3})(\d{2})(\d{2})$',
                lambda m: f"{int(m.group(1)) + 1911}-{int(m.group(2)):02d}-{int(m.group(3)):02d}",
                regex=True
            )
            original = original.str.replace(
                r'^(\d{2})(\d{2})(\d{2})$',
                lambda m: f"{int(m.group(1)) + 1911}-{int(m.group(2)):02d}-{int(m.group(3)):02d}",
                regex=True
            )
            original = original.str.replace(
                r'^(\d{2,3})[./-](\d{1,2})[./-](\d{1,2})$',
                lambda m: f"{int(m.group(1)) + 1911}-{int(m.group(2)):02d}-{int(m.group(3)):02d}",
                regex=True
            )
            # 年月補日01，包含兩位民國年例外處理
            original = original.str.replace(
                r'^(\d{4})[./-](\d{1,2})$',
                r'\1-\2-01',
                regex=True
            )
            original = original.str.replace(
                r'^(\d{2})(\d{2})$',
                lambda m: f"{int(m.group(1)) + 1911}-{int(m.group(2)):02d}-01",
                regex=True
            )
            # 西元兩位年判斷 19xx / 20xx
            original = original.str.replace(
                r'^(\d{2})[./-](\d{1,2})[./-](\d{1,2})$',
                lambda m: (
                    f"20{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
                    if int(m.group(1)) < 30
                    else f"19{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
                ),
                regex=True
            )

            # 轉 datetime 並避免空值變 float（空值會是 NaT）
            dt_col = pd.to_datetime(original, format='%Y-%m-%d', errors='coerce')

            total = len(df)
            nulls = dt_col.isna().sum()
            print(f"📆 成功轉換：{total - nulls} 筆，NaT：{nulls} 筆")
            if nulls > 0:
                print("⚠️ 無法轉換樣本（前5筆）：")
                print(df.loc[dt_col.isna(), col].head(5))

            # 補值：只有在 dt_col 是 datetime，且有有效值才補
            valid = dt_col.dropna()
            if not valid.empty:
                if fill_strategy == 'mode':
                    fill_value = valid.mode().iloc[0]
                elif fill_strategy == 'mean':
                    fill_value = valid.mean()
                elif fill_strategy == 'median':
                    fill_value = valid.median()
                else:
                    print(f"⚠️ 不支援的補值策略：{fill_strategy}")
                    fill_value = None

                if fill_value is not None:
                    dt_col = dt_col.fillna(fill_value)
                    print(f"✅ 使用 {fill_strategy} 補值 NaT → {fill_value.date()}")

            df[col] = dt_col
        except Exception as e:
            print(f"❌ 處理欄位 {col} 發生錯誤:")
            print("錯誤訊息:", e)
            print(traceback.format_exc())

    if save:
        try:
            output_path = parquet_path if overwrite else parquet_path.replace('.parquet', '_fixed.parquet')
            df.to_parquet(output_path, index=False)
            print(f"💾 已儲存修正檔至：{output_path}")
        except Exception as e:
            print("❌ 儲存檔案失敗")
            print("錯誤訊息:", e)
            print(traceback.format_exc())

    return df



# import os
# from your_module import convert_single_csv_to_parquet  # 如果函式在其他檔案中

if __name__ == '__main__':
    path = os.path.dirname(__file__)
    path = os.path.join(path, 'data_csv')

    for filename in os.listdir(path):
        if filename.endswith('.csv'):
            fullpath = os.path.join(path, filename)
            print(f"找到 .csv 的檔案: {fullpath}")

            try:
                result = convert_single_csv_to_parquet(
                    csv_path=fullpath,
                    skiprows=1,
                    output_path=None
                )
                if result:
                    print(f"✅ 成功轉換：{result}")
                    fix_date_columns_custom(parquet_path = result,
                                            fill_strategy='none',
                                            save=True,
                                            overwrite=False)
                else:
                    print(f"⚠️ 轉換失敗：{fullpath}")

            except Exception as e:
                print(f"❌ 發生例外錯誤，無法處理檔案：{fullpath}\n原因：{e}")
