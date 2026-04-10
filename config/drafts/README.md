# 드래프트 설정 파일

각 시즌의 드래프트 정보를 JSON 파일로 관리합니다.

## 파일 명명 규칙

- `{연도}_main.json` - 시즌 시작 전 메인 드래프트
- `{연도}_supplemental.json` - 올스타 브레이크 2차 드래프트

## JSON 구조

```json
{
  "season": 2025,
  "draft_type": "main",           // "main" 또는 "supplemental"
  "application_date": "2025-03-21", // WAR 계산 시작일 (YYYY-MM-DD)
  "description": "설명",
  
  // 팀 퐈(FA) 계산 설정 (선택사항 - 없으면 기본값 사용)
  "fa_config": {
    "roster_size": 29,            // 기본 FA 로스터 크기
    "supplemental_bonus": 5,      // 2차 드래프트 후 추가 인원
    "position_requirements": {
      "P": 11,                    // 투수 최소 인원
      "C": 2,                     // 포수 최소 인원
      "IF": 7,                    // 내야수 최소 인원 (1B, 2B, SS, 3B)
      "OF": 5                     // 외야수 최소 인원 (LF, CF, RF)
    }
  },
  
  "picks": [
    {
      "team": "준",               // 팀 ID (준, 뚝, 삼, 언, 엉, 옥, 코, 우)
      "round": "용병타",          // 라운드명
      "player_id": "15004",       // Statiz 선수 ID
      "player_name": "김도영"     // 선수 이름 (참조용)
    }
  ]
}
```

## 드래프트 타입

### main (메인 드래프트)
- 시즌 시작 전 진행
- 라운드: 용병타, 용병투1, 용병투2, 1R ~ 25R
- `application_date`부터 WAR 계산에 포함

### supplemental (2차 드래프트)
- 올스타 브레이크 기간 중 진행 (보통 7월 중순)
- FA 풀에서 선수 추가 지명
- 라운드: 2차1R, 2차2R, ...
- `application_date`부터 WAR 계산에 포함 (이전 기록은 제외)
- **2차 드래프트 이후 팀 퐈는 `supplemental_bonus`만큼 추가 인원 포함**

## 팀 퐈 (FA) WAR 계산 방식

### 기본 규칙
1. **FA 상태에서 쌓은 WAR만 반영**: 어떤 팀에도 소속되지 않은 기간에 쌓은 WAR만 계산
2. **방출 시 WAR 기록**: 선수가 방출되면 그 시점의 WAR을 기록하고, 이후 쌓은 WAR만 FA에 반영
3. **드래프트되지 않은 선수**: 시즌 시작부터 전체 WAR이 FA에 반영

### 로스터 선발
- 포지션별 최소 인원을 먼저 FA WAR 순으로 선발
- 나머지 인원은 포지션 무관하게 FA WAR 순으로 선발
- 2차 드래프트 이후에는 `supplemental_bonus`만큼 추가 선발 (포지션 무관)

### 예시
```
기본: 29명 (투수11 + 포수2 + 내야7 + 외야5 + 자유4)
2차 드래프트 후: 34명 (29 + 5)
```

## 적용 방법

```bash
# 메인 드래프트 적용
python -m app.core.draft_loader config/drafts/2025_main.json

# 2차 드래프트 적용
python -m app.core.draft_loader config/drafts/2025_supplemental.json

# 모든 드래프트 적용 (해당 연도)
python -m app.core.draft_loader --year 2025
```

## 선수 ID 찾기

Statiz에서 선수 페이지 URL의 숫자가 선수 ID입니다:
- URL: `http://statiz.co.kr/player/?opt=1&name=...&birth=...&pid=15004`
- 선수 ID: `15004`
