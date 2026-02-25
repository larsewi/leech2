#ifndef __LEECH2_H__
#define __LEECH2_H__

#include <stddef.h>
#include <stdint.h>

extern int lch_init(const char *work_dir);
extern int lch_block_create(void);
extern int lch_patch_create(const char *hash, uint8_t **buf, size_t *len);
extern int lch_patch_to_sql(const uint8_t *buf, size_t len, char **sql);
extern int lch_patch_applied(uint8_t *buf, size_t len, int reported);
extern void lch_free_sql(char *sql);

#endif /* __LEECH2_H__ */
