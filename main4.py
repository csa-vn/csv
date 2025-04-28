import pandas as pd
import os
import glob
import json

# --- CẤU HÌNH THƯ MỤC VÀ CỘT ---
folder_path = r'C:\Users\DELL\Desktop\csv\cttp' # Thay đổi nếu cần
company_col = 'Company name' # Thay đổi nếu tên cột công ty khác
phone_col = 'Phone'        # Thay đổi nếu tên cột số điện thoại khác
email_cols = ['Email 1', 'Email 2', 'Email 3'] # Thay đổi nếu tên cột email khác hoặc số lượng khác
# -------------------------------

print(f"Đang xử lý các tệp CSV trong thư mục: {folder_path}")

search_pattern = os.path.join(folder_path, '*.csv')
csv_files = glob.glob(search_pattern)

# --- Bước 1: Tìm tệp CSV ---
print(f"\n--- Bước 1: Tìm tệp CSV ---")
if not csv_files:
    print(f"Không tìm thấy tệp CSV nào trong thư mục: {folder_path}")
    exit() # Thoát chương trình ngay
else:
    print(f"Tìm thấy {len(csv_files)} tệp CSV:")
    for f in csv_files:
        print(f"- {f}")
print("--- Kết thúc Bước 1 ---")

# Bắt đầu xử lý chỉ khi có tệp CSV được tìm thấy
print(f"\n--- Bước 2: Đọc và kết hợp tệp ---")
dfs_list = [] # Danh sách để lưu trữ các DataFrame từ mỗi tệp
last_processed_csv = None # Biến để lưu lại tên tệp CSV cuối cùng

for file_path in csv_files:
    try:
        # Thử các encoding phổ biến cho tiếng Việt
        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig', engine='python', on_bad_lines='warn')
        except UnicodeDecodeError:
            print(f"Không thể đọc {file_path} bằng utf-8-sig. Thử cp1258...")
            df = pd.read_csv(file_path, encoding='cp1258', engine='python', on_bad_lines='warn')
        except Exception as e:
             print(f"Lỗi đọc tệp {file_path} (không phải UnicodeDecodeError): {e}")
             continue # Bỏ qua tệp này và chuyển sang tệp tiếp theo

        dfs_list.append(df)
        last_processed_csv = file_path # Cập nhật tệp cuối cùng đã xử lý thành công
        print(f"Đã đọc thành công: {file_path} ({len(df)} dòng)")
    except FileNotFoundError:
        print(f"Lỗi: Không tìm thấy tệp {file_path}. Có thể tệp đã bị xóa hoặc đổi tên.")
    except Exception as e:
        print(f"Lỗi khi đọc tệp {file_path}: {e}")


# Kiểm tra xem có DataFrame nào đã được đọc thành công không
if not dfs_list:
    print("\nKhông có dữ liệu nào được đọc thành công từ các tệp CSV. Không thể tiếp tục xử lý.")
else:
    combined_df = pd.concat(dfs_list, ignore_index=True)

    # --- Kiểm tra dữ liệu gốc sau khi kết hợp ---
    print("\n--- Kiểm tra DataFrame gốc kết hợp ---")
    print(f"Tổng số dòng trong DataFrame gốc: {len(combined_df)}")
    # print("\n5 dòng đầu:")
    # print(combined_df.head().to_markdown(index=False))
    print("--- Kết thúc kiểm tra DataFrame gốc ---")

    print("\n--- Bắt đầu xử lý tìm các dòng dữ liệu trùng tên và có thông tin liên hệ ---")

    # Kiểm tra các cột cần thiết
    all_contact_cols = [phone_col] + email_cols
    required_cols = [company_col] + all_contact_cols
    if not all(col in combined_df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in combined_df.columns]
        print(f"\nLỖI NGHIÊM TRỌNG: Các cột cần thiết ({', '.join(missing)}) không tồn tại trong dữ liệu kết hợp.")
        print("Vui lòng kiểm tra lại tên cột trong tệp CSV của bạn và sửa lại các biến company_col, phone_col, email_cols trong code cho khớp.")

    else:
        # --- Bước 3: Làm sạch dữ liệu liên hệ và tên công ty ---
        print("\n--- Bước 3: Làm sạch dữ liệu liên hệ và tên công ty ---")
        for col in all_contact_cols:
            # Chuyển sang string, điền giá trị rỗng bằng '', và loại bỏ khoảng trắng thừa
             combined_df[col] = combined_df[col].astype(str).fillna('').str.strip()
             # print(f"Đã làm sạch cột: {col}")

        # Xử lý cột tên công ty: chuyển sang string, loại bỏ khoảng trắng
        combined_df[company_col] = combined_df[company_col].astype(str).str.strip()
        print(f"Đã làm sạch cột: {company_col}")
        print("--- Kết thúc Bước 3 ---")

        # --- Bước 4: Xác định tên công ty trùng lặp trong dữ liệu gốc ---
        print("\n--- Bước 4: Xác định tên công ty trùng lặp ---")
        # Đếm số lần xuất hiện tên công ty trong dữ liệu gốc
        company_counts = combined_df.groupby(company_col).size()
        # Lọc các tên công ty rỗng và chỉ lấy các tên có số đếm > 1
        valid_company_counts = company_counts[company_counts.index != '']
        duplicate_company_names = valid_company_counts[valid_company_counts > 1].index.tolist()

        if not duplicate_company_names:
            print("Không tìm thấy tên công ty nào bị trùng lặp trong dữ liệu gốc (loại trừ các dòng không có tên công ty rỗng và chỉ xuất hiện 1 lần).")
            final_output_df = pd.DataFrame() # Tạo DF rỗng để bỏ qua bước xuất JSON
        else:
            print(f"Tìm thấy {len(duplicate_company_names)} tên công ty bị trùng lặp.")
            # Chỉ in danh sách nếu không quá dài
            if len(duplicate_company_names) < 50:
                 print("\n".join([f"- {name}" for name in duplicate_company_names]))
            else:
                 print(f" (Danh sách quá dài, chỉ in số lượng: {len(duplicate_company_names)})")

            print("--- Kết thúc Bước 4 ---")

            # --- Bước 5: Lọc các dòng gốc thỏa mãn điều kiện trùng tên VÀ có dữ liệu liên hệ ---
            print("\n--- Bước 5: Lọc các dòng gốc thỏa mãn điều kiện trùng tên VÀ có dữ liệu liên hệ ---")

            # Lọc các dòng gốc chỉ lấy những dòng có tên công ty nằm trong danh sách trùng lặp
            duplicate_name_rows_df = combined_df[
                combined_df[company_col].isin(duplicate_company_names)
            ].copy() # Dùng .copy() để tránh SettingWithCopyWarning

            print(f"Số lượng dòng gốc có tên công ty trùng lặp: {len(duplicate_name_rows_df)}")

            if duplicate_name_rows_df.empty:
                 print("Không có dòng nào có tên công ty trùng lặp trong dữ liệu gốc.")
                 final_output_df = pd.DataFrame() # Tạo DF rỗng
            else:
                 # Kiểm tra nếu CÓ BẤT KỲ cột liên hệ nào (Phone, Email1, Email2, Email3...) KHÔNG RỖNG trong dòng đó
                 # Tạo điều kiện lọc: True nếu ít nhất một cột liên hệ không rỗng cho dòng đó
                 # .any(axis=1) kiểm tra xem có bất kỳ giá trị True nào trên mỗi hàng (axis=1) không
                 has_contact_info_condition = (duplicate_name_rows_df[all_contact_cols] != '').any(axis=1)

                 # Áp dụng điều kiện lọc để chỉ giữ lại các dòng có thông tin liên hệ
                 final_output_df = duplicate_name_rows_df[has_contact_info_condition].copy()

                 print(f"Số lượng dòng gốc có tên công ty trùng lặp VÀ có thông tin liên hệ: {len(final_output_df)}")

                 if final_output_df.empty:
                      print("Không có dòng nào có tên công ty trùng lặp và có thông tin liên hệ sau khi lọc.")
                 else:
                      print("5 dòng đầu của DataFrame cuối cùng (dòng gốc, tên trùng, có liên hệ):")
                      # In 5 dòng đầu với tất cả các cột gốc
                      print(final_output_df.head().to_markdown(index=False, numalign="left", stralign="left"))

            print(f"\nTổng số dòng trong DataFrame cuối cùng (dòng gốc, tên trùng, có liên hệ): {len(final_output_df)}")
            print("--- Kết thúc Bước 5 ---")

        # --- Bước 6: Xuất dữ liệu ra tệp JSON ---
        print(f"\n--- Bước 6: Xuất dữ liệu ra tệp JSON ---")

        # Chỉ xuất nếu có dữ liệu trong final_output_df và có tệp CSV được xử lý thành công
        if not final_output_df.empty and last_processed_csv:
            # Xác định tên tệp JSON dựa trên tên tệp CSV cuối cùng đã được xử lý thành công
            base_name = os.path.splitext(os.path.basename(last_processed_csv))[0] # Lấy tên tệp không kèm đuôi
            json_output_filename = f"{base_name}_duplicate_info.json" # Thêm hậu tố để rõ ràng
            json_output_path = os.path.join(folder_path, json_output_filename)

            try:
                # Chuyển DataFrame sang định dạng JSON list of records
                # force_ascii=False để đảm bảo tiếng Việt hiển thị đúng
                # index=False để không bao gồm index của DataFrame trong JSON
                final_output_df.to_json(json_output_path, orient='records', indent=4, force_ascii=False, index=False)
                print(f"Đã xuất thông tin các dòng gốc (tên trùng, có dữ liệu liên hệ) ra tệp: {json_output_path}")
            except Exception as e:
                print(f"Lỗi khi ghi tệp JSON {json_output_path}: {e}")
        elif final_output_df.empty:
             print("Không có dữ liệu công ty trùng lặp có thông tin liên hệ để xuất ra tệp JSON.")
        else: # last_processed_csv is None, meaning no files were successfully read
             print("Không có tệp CSV nào được đọc thành công để xác định tên tệp JSON đầu ra.")


        print("\n--- KẾT THÚC XỬ LÝ DỮ LIỆU TRÙNG LẶP ---")

print("\n--- Quá trình xử lý hoàn tất ---")