#ifndef __LEECH2_H__
#define __LEECH2_H__

#include <stddef.h>
#include <stdint.h>

extern int lch_init(const char *work_dir);
extern int lch_block_create(void);
extern int lch_patch_create(const char *last_known, uint8_t **patch, size_t *patch_len);
extern int lch_patch_to_sql(const uint8_t *patch, size_t patch_len, char **sql);
extern void lch_free_patch(uint8_t *patch, size_t patch_len);
extern void lch_free_str(char *str);

#endif /* __LEECH2_H__ */
