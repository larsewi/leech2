#ifndef __LEECH2_H__
#define __LEECH2_H__

#include <stddef.h>
#include <stdint.h>

typedef struct Config lch_config_t;

extern lch_config_t *lch_init(const char *work_dir);
extern void lch_deinit(lch_config_t *config);
extern int lch_block_create(const lch_config_t *config);
extern int lch_patch_create(const lch_config_t *config, const char *hash, uint8_t **buf, size_t *len);
extern int lch_patch_to_sql(const lch_config_t *config, const uint8_t *buf, size_t len, char **sql);
extern int lch_patch_applied(const lch_config_t *config, uint8_t *buf, size_t len, int reported);
extern void lch_free_sql(char *sql);

#endif /* __LEECH2_H__ */
