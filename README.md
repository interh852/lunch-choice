# lunch-choice
社内のお弁当の選択・集計するアプリ
<img src="figures/お弁当アプリ.svg" width="100%" />

# Development

```
cd docker
docker compose up -d --build
```

# Build

Google Cloudで本番運用するので、下記の方法でDockerイメージをビルドしArtifact Registryで管理
https://cloud.google.com/build/docs/automating-builds/github/build-repos-from-github?hl=ja&generation=1st-gen

# Script

メニュー表の作成
```
cd src
python run.py -o create
```

