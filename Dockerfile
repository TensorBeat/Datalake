FROM golang:1.15 as dev


FROM dev

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build ./cmd/server

ENTRYPOINT [ "./server" ]