# Studio Rats canon correction

On September 29, 2026, 6 Bit clarified that Studio Rats are cats. The name comes
from 6 Bit not knowing people kept cats as pets. The studio-infestation joke
describes his perspective, not the animals' species.

The fresh private artwork preview on PR618 generated a literal rat because BNL's
image prompt explicitly requested one. The renderer followed that prompt. The
shared lore entry said "Studio infestation. Some dimensions call them cats,"
which left the actual species ambiguous.

This correction changes that entry in the existing canon/source owner. It
states that the animals are cats and explains the nickname. Conversation and
the existing private/Ambient art paths already consume this same lore function.
There is no art-only replacement table, forced scene, breed, appearance or style.
No memory rewrite or extra generation request is part of this change.

The returned image was inspected and its SHA-256 matches the preview receipt:
`15e32901a50b4d5461585cbe5638a4c2e57ba6bfeebd36397e3d95ccd556861f`.
6 Bit liked the artwork's visual result and corrected the species. The image
remains the original private draft; it is not evidence of correct Studio Rats
canon or completed Discord/website delivery. The separate containment-memory
inspiration remains to be checked against its source receipt.

## Normal post-merge deployment

Use the existing service checkout and environment:

```bash
cd /home/ubuntu/bnl01 &&
git pull --ff-only origin main &&
sudo systemctl restart bnl01 &&
sudo systemctl is-active bnl01
```

## Focused post-deploy evidence

1. Record the deployed commit and active service result from the normal rollout.
2. On the next intended Studio Rats conversation or artwork, check that BNL
   understands they are cats and retains 6 Bit's nickname explanation. The
   output need not use exact wording or introduce this subject unprompted.
3. If another image is explicitly requested, inspect BNL's new image prompt and
   the returned pixels separately. Use the existing private preview tool and a
   new directory. Do not regenerate the accepted visual concept merely to
   validate a text edit.

The optional Ambient artwork ceiling and activation state are unchanged.
