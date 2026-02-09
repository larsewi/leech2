#ifndef __LEECH2_H__
#define __LEECH2_H__

#include <stddef.h>
#include <stdint.h>

extern int lch_init(const char *work_dir);
extern int lch_block_create(void);
extern int lch_patch_create(const char *block, uint8_t **out, size_t *out_len);
extern void lch_free(uint8_t *ptr, size_t len);

#endif /* __LEECH2_H__ */
