import pandas as pd
import os
import glob
import json

folder_path = r'C:\Users\DELL\Desktop\csv\ctdv'

standard_company_col = 'Company name'

column_name_map = {
}

email_cols = ['Email 1', 'Email 2', 'Email 3']

phone_col = 'Phone'

print(f"--- BẮT ĐẦU QUÁ TRÌNH XỬ LÝ ---")
print(f"Mục tiêu: Kết hợp dữ liệu, gộp SĐT/Email theo Công Ty ({standard_company_col}), lọc bỏ công ty không có SĐT/Email, xuất JSON.")
print(f"Đang xử lý các tệp CSV trong thư mục: {folder_path}")
print(f"Kiểm tra 'column_name_map', 'email_cols' và 'phone_col' trong code để đảm bảo cấu hình đúng.")

search_pattern = os.path.join(folder_path, '*.csv')
csv_files = glob.glob(search_pattern)

print(f"\n--- Bước 1: Tìm tệp CSV ---")
if not csv_files:
    print(f"Không tìm thấy tệp CSV nào trong thư mục: {folder_path}")
    print("--- QUÁ TRÌNH XỬ LÝ KẾT THÚC ---")
    exit()
else:
    print(f"Tìm thấy {len(csv_files)} tệp CSV:")
    for i, f in enumerate(csv_files):
        print(f"- {i+1}: {os.path.basename(f)}")
print("--- Kết thúc Bước 1 ---")

print(f"\n--- Bước 2: Đọc, chuẩn hóa tên cột và kết hợp tệp ---")
dfs_list = []

for i, file_path in enumerate(csv_files):
    print(f"\n[{i+1}/{len(csv_files)}] Đang xử lý tệp: {os.path.basename(file_path)}")
    df = None
    try:
        print(f"  Đang đọc bằng utf-8-sig...")
        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig', engine='python', on_bad_lines='warn')
        except UnicodeDecodeError:
            print(f"  Không thể đọc bằng utf-8-sig. Thử cp1258...")
            try:
                df = pd.read_csv(file_path, encoding='cp1258', engine='python', on_bad_lines='warn')
            except UnicodeDecodeError:
                print(f"  Không thể đọc bằng cp1258. Thử latin-1...")
                try:
                    df = pd.read_csv(file_path, encoding='latin-1', engine='python', on_bad_lines='warn')
                except UnicodeDecodeError:
                    print(f"  Không thể đọc tệp '{os.path.basename(file_path)}' với các encoding thông thường. Bỏ qua tệp này.")
                    continue
                except Exception as e:
                    print(f"  Lỗi đọc tệp '{os.path.basename(file_path)}' sau khi thử cp1258: {e}")
                    continue
            except Exception as e:
                print(f"  Lỗi đọc tệp '{os.path.basename(file_path)}' sau khi thử utf-8-sig: {e}")
                continue
        except Exception as e:
            print(f"  Lỗi đọc tệp '{os.path.basename(file_path)}' (không phải UnicodeDecodeError): {e}")
            continue

        if df is not None:
            df.columns = df.columns.str.strip()

            rename_dict = {col.strip(): column_name_map[col.strip()] for col in column_name_map if col.strip() in df.columns}
            if rename_dict:
                df = df.rename(columns=rename_dict)
                print(f"  Tên cột sau khi đổi tên: {df.columns.tolist()}")


            if standard_company_col not in df.columns:
                print(f"  CẢNH BÁO: Tệp '{os.path.basename(file_path)}' thiếu cột tên công ty chuẩn '{standard_company_col}' sau khi chuẩn hóa tên. Dữ liệu từ tệp này sẽ không được tính vào dữ liệu chung.")
                continue

            required_contact_cols = [phone_col] + email_cols
            missing_contact_cols = [col for col in required_contact_cols if col not in df.columns]
            if missing_contact_cols:
                print(f"  CẢNH BÁO: Tệp '{os.path.basename(file_path)}' thiếu các cột liên hệ được cấu hình: {missing_contact_cols}. Dữ liệu liên hệ từ tệp này có thể không đầy đủ.")


            dfs_list.append(df)
            print(f"  Đã xử lý xong việc đọc và chuẩn hóa tên cột cho tệp: {os.path.basename(file_path)} ({len(df)} dòng)")

    except FileNotFoundError:
        print(f"Lỗi: Không tìm thấy tệp {os.path.basename(file_path)}. Có thể tệp đã bị xóa hoặc đổi tên.")
    except Exception as e:
        print(f"Lỗi không mong muốn khi xử lý tệp {os.path.basename(file_path)}: {e}")


if not dfs_list:
    print("\nKhông có dữ liệu nào được đọc thành công từ các tệp CSV có cột tên công ty cần thiết.")
    print("--- QUÁ TRÌNH XỬ LÝ KẾT THÚC ---")
else:
    print("\nĐang kết hợp các DataFrame...")
    combined_df = pd.concat(dfs_list, ignore_index=True)
    print("Đã kết hợp xong.")

    print("\n--- Kiểm tra DataFrame gốc kết hợp ---")
    print(f"Tổng số dòng trong DataFrame gốc: {len(combined_df)}")
    print("--- Kết thúc kiểm tra DataFrame gốc ---")

    print("\n--- Bước 3: Chuẩn bị dữ liệu trước khi nhóm và tổng hợp ---")

    if standard_company_col not in combined_df.columns:
        print(f"\nLỖI NGHIÊM TRỌNG: Cột tên công ty chuẩn '{standard_company_col}' không tồn tại trong dữ liệu kết hợp sau khi xử lý.")
        print("Vui lòng kiểm tra lại tên cột trong tệp CSV của bạn và cấu hình 'column_name_map' cho đúng.")
        print("--- QUÁ TRÌ LÝ KẾT THÚC VỚI LỖI ---")
    else:
        combined_df[standard_company_col] = combined_df[standard_company_col].astype(str).fillna('').str.strip()
        combined_df = combined_df[combined_df[standard_company_col] != ''].copy()
        print(f"Đã làm sạch và loại bỏ các dòng có tên công ty rỗng. Số dòng còn lại: {len(combined_df)}")

        contact_cols_exist = [col for col in [phone_col] + email_cols if col in combined_df.columns]
        for col in contact_cols_exist:
            combined_df[col] = combined_df[col].astype(str).fillna('').str.strip()
        print(f"Đã làm sạch các cột liên hệ ({contact_cols_exist}).")

        print("--- Kết thúc Bước 3 ---")

        print(f"\n--- Bước 4: Nhóm dữ liệu theo '{standard_company_col}' và tổng hợp ---")

        agg_dict = {}
        for col in combined_df.columns:
            if col == standard_company_col:
                continue
            elif col == phone_col:
                agg_dict[col] = lambda x: list(set(x) - {''})
            elif col in email_cols:
                agg_dict[col] = lambda x: list(set(x) - {''})
            else:
                agg_dict[col] = 'first'


        grouped_data = combined_df.groupby(standard_company_col, as_index=False).agg(agg_dict)

        print(f"Đã nhóm dữ liệu. Số lượng công ty duy nhất ban đầu: {len(grouped_data)}")
        print("--- Kết thúc Bước 4 ---")


        print("\n--- Bước 5: Xử lý sau tổng hợp (gộp Email, lọc, chuẩn bị xuất) ---")

        if email_cols:
            email_cols_exist_grouped = [col for col in email_cols if col in grouped_data.columns]
            if email_cols_exist_grouped:
                grouped_data['Emails'] = grouped_data[email_cols_exist_grouped].apply(
                    lambda row: list(set(item for sublist in row for item in sublist)), axis=1
                )
                grouped_data = grouped_data.drop(columns=email_cols_exist_grouped)
                print(f"Đã gộp các cột Email ({email_cols_exist_grouped}) thành cột 'Emails'.")
            else:
                grouped_data['Emails'] = [[]] * len(grouped_data)
                print("Không tìm thấy các cột Email cấu hình sau khi nhóm. Tạo cột 'Emails' rỗng.")
        else:
            grouped_data['Emails'] = [[]] * len(grouped_data)
            print("Không có cột Email nào được cấu hình. Tạo cột 'Emails' rỗng.")


        if phone_col not in grouped_data.columns:
            grouped_data[phone_col] = [[]] * len(grouped_data)
            print(f"Không tìm thấy cột SĐT ('{phone_col}') sau khi nhóm. Tạo cột '{phone_col}' rỗng.")


        if phone_col in grouped_data.columns:
            grouped_data = grouped_data.rename(columns={phone_col: 'Phones'})
            print(f"Đã đổi tên cột '{phone_col}' thành 'Phones'.")


        initial_grouped_count = len(grouped_data)
        filtered_data = grouped_data[
            (grouped_data['Phones'].apply(len) > 0) | (grouped_data['Emails'].apply(len) > 0)
        ].copy()

        final_filtered_count = len(filtered_data)
        print(f"Số lượng công ty sau khi lọc bỏ các công ty không có SĐT/Email: {final_filtered_count}")
        print(f"Đã loại bỏ {initial_grouped_count - final_filtered_count} công ty không có thông tin liên hệ.")

        final_cols_order = [standard_company_col, 'Phones', 'Emails'] + [
            col for col in filtered_data.columns if col not in [standard_company_col, 'Phones', 'Emails']
        ]
        final_cols_order = [col for col in final_cols_order if col in filtered_data.columns]

        filtered_data = filtered_data[final_cols_order]
        print("Đã sắp xếp lại thứ tự các cột.")

        print("--- Kết thúc Bước 5 ---")


        print(f"\n--- Bước 6: Xuất dữ liệu ra tệp JSON ---")

        if not filtered_data.empty:
            json_records = filtered_data.to_dict('records')

            folder_base_name = os.path.basename(folder_path)
            json_output_filename = f"{folder_base_name}_merged_contact_data.json"
            json_output_path = os.path.join(folder_path, json_output_filename)

            try:
                print(f"Đang xuất dữ liệu đã xử lý ra tệp JSON: {json_output_path}")
                with open(json_output_path, 'w', encoding='utf-8') as f:
                    json.dump(json_records, f, indent=4, ensure_ascii=False)

                print(f"Đã xuất dữ liệu đã xử lý ra tệp: {json_output_path}")
            except Exception as e:
                print(f"Lỗi khi ghi tệp JSON {json_output_path}: {e}")
        else:
            print("Không có dữ liệu nào còn lại sau khi lọc công ty không có thông tin liên hệ. Không xuất tệp JSON.")


        print("\n--- KẾT THÚC XỬ LÝ DỮ LIỆU ---")

print("\n--- Quá trình xử lý hoàn tất ---")