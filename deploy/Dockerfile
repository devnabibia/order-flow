FROM golang:1.22 AS build
WORKDIR /src
COPY . .
# Resolves indirect deps and go.sum inside the build, so a local `go mod tidy` is optional for Docker.
RUN go mod tidy
ARG SERVICE
RUN CGO_ENABLED=0 go build -o /out/app ./services/${SERVICE}

FROM gcr.io/distroless/static
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
