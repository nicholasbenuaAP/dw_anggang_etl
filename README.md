# Membuat Virtual Environment

Untuk membuat **virtual environment (venv)** di Python, langkahnya cukup sederhana. Berikut panduan praktisnya:

### 1. Pastikan Python sudah terpasang
Cek versi Python di terminal:
```bash
python3 --version
```
atau
```bash
python --version
```

### 2. Buat virtual environment
Gunakan perintah:
```bash
python3 -m venv nama_env
```
- `nama_env` bisa diganti sesuai kebutuhan, misalnya `venv`, `env`, atau `myproject_env`.

Contoh:
```bash
python3 -m venv venv
```

### 3. Aktifkan virtual environment
- **Linux/MacOS:**
  ```bash
  source venv/bin/activate
  ```
- **Windows (Command Prompt):**
  ```cmd
  venv\Scripts\activate
  ```
- **Windows (PowerShell):**
  ```powershell
  .\venv\Scripts\Activate.ps1
  ```

Jika berhasil, di terminal akan muncul prefix `(venv)` menandakan environment aktif.

### 4. Nonaktifkan virtual environment
Untuk keluar dari venv:
```bash
deactivate
```

---

💡 Tips tambahan:
- Gunakan venv agar setiap proyek Python punya **dependensi terpisah**, tidak bercampur dengan sistem global.
- Biasanya folder `venv` ditambahkan ke `.gitignore` agar tidak ikut diunggah ke repository.

# Untuk Aktivasi


### 1. PowerShell (Default di VS Code)
Ketik perintah ini dan tekan Enter:
```powershell
.\.venv\Scripts\Activate.ps1
```
*Jika muncul error "Scripts is not digitally signed", jalankan perintah ini dulu sekali saja: `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process` lalu ulangi perintah aktivasi.*

### 2. Command Prompt (CMD)
Ketik perintah ini dan tekan Enter:
```cmd
.venv\Scripts\activate
```

### 3. Git Bash
Ketik perintah ini dan tekan Enter:
```bash
source .venv/Scripts/activate
```

---

**Tanda jika sudah aktif:**
Anda akan melihat teks `(.venv)` muncul di awal baris perintah di terminal Anda, seperti ini:
`(.venv) PS C:\dev\data_warehouse\etl_process>`


Kemudian Jalankan:
```powershell
pip install sqlalchemy pandas psycopg2-binary prefect
```