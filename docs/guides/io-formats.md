# I/O Formats

FrameKit supports multiple data formats:

- CSV (`fromCSV`, `toCSV`)
- JSON / NDJSON (`fromJSON`, `toJSON`, `toNDJSON`)
- Arrow IPC (`fromArrow`, `toArrow`)
- Excel (`fromExcel`, `toExcel`)
- Parquet (`fromParquet`, `toParquet`)
- SQL export (`toSQL`)

```ts
import { DataFrame } from 'framekit';

const df = await DataFrame.fromCSV('./input.csv');
await df.toParquet('./output.parquet');
```
