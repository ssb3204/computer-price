# Ship

작업 완료 후 테스트 → 커밋 → 푸시 → PR 생성 → 메모리 업데이트까지 한 번에 처리하는 스킬.

## 실행 순서

1. **테스트 실행**
   ```bash
   python -m pytest tests/ -v -o "addopts="
   ```
   - 실패하면 중단하고 오류를 사용자에게 보고. PR로 넘어가지 않음.

2. **커밋**
   - `git status`로 staged 파일 확인
   - staged 없으면 사용자에게 알리고 중단
   - Conventional Commit 형식으로 메시지 작성: `feat:` / `fix:` / `refactor:` / `docs:` / `test:` / `perf:`
   - Co-Authored-By 추가하지 않음

3. **푸시**
   ```bash
   git push
   ```
   - 브랜치가 원격에 없으면 `git push -u origin <branch>`

4. **PR 생성**
   - `gh pr create`로 PR 생성
   - 제목: 커밋 메시지 기반으로 간결하게
   - 본문: 변경 내용 요약 + 테스트 체크리스트 (한국어)
   - master가 base 브랜치

5. **메모리 업데이트**
   - `C:\Users\ryanp\.claude\projects\C--Users-ryanp-OneDrive-------computer-price\memory\project_progress.md` 에 완료 사항 반영
   - PR 번호, 브랜치명, 작업 내용 기록
