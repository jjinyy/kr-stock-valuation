## consensus

국내 상장기업(주 1회 갱신) 대상으로,
FnGuide 컨센서스(26년 PBR/PER/EPS) + 현재주가(네이버 등) 기반으로
적정주가 및 괴리율을 테이블로 보여주는 웹앱.

### 목표 화면 컬럼

기업명 / 현재주가 / PBR / PER / EPS / 적정주가 / 괴리율((적정주가-현재주가)/현재주가)

### 실행(개발)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

브라우저에서 `http://127.0.0.1:8000` 접속.

