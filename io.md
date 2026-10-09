| Cache Engine | Addressing Pattern |
| :-: | :-: |
| Block-based | (fd, offset, buffer) |
| KV-based | (key, buffer) |


| Io Engine | Compatible Devices | Acceptable Addressing Pattern |
| :-: | :-: | :-: |
| Psync | FS, File | (fd, offset, buffer) |
| Uring | FS, File | (fd, offset, buffer) |
| OpenDAL | (x, OpenDAL only) | (key, buffer) |

| Device | Acceptable Addressing Pattern |
| :-: | :-: |
| Fs | (fd, offset, buffer), (key, buffer) |
| File | (fd, offset, buffer) |
| OpenDAL | (key, buffer) |